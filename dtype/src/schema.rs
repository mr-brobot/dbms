//! Schema type for the DBMS query engine.

use crate::Field;
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};

/// A schema consisting of a list of fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Project the schema to a subset of fields by index.
    pub fn project(&self, indices: &[usize]) -> Self {
        Self {
            fields: indices.iter().map(|&i| self.fields[i].clone()).collect(),
        }
    }

    /// Select fields by name.
    pub fn select(&self, names: &[&str]) -> Result<Self, String> {
        let fields = names
            .iter()
            .map(|name| {
                self.fields
                    .iter()
                    .find(|f| f.name() == *name)
                    .cloned()
                    .ok_or_else(|| format!("field not found: {}", name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { fields })
    }
}

impl From<Schema> for ArrowSchema {
    fn from(s: Schema) -> Self {
        let fields = s
            .fields
            .into_iter()
            .map(ArrowField::from)
            .collect::<Vec<_>>();
        ArrowSchema::new(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ])
    }

    #[test]
    fn test_schema_project() {
        let schema = test_schema();
        let projected = schema.project(&[2, 0]);

        assert_eq!(projected.fields().len(), 2);
        assert_eq!(projected.fields()[0].name(), "c");
        assert_eq!(projected.fields()[1].name(), "a");
    }

    #[test]
    fn test_schema_select() {
        let schema = test_schema();
        let selected = schema.select(&["b", "a"]).unwrap();

        assert_eq!(selected.fields().len(), 2);
        assert_eq!(selected.fields()[0].name(), "b");
        assert_eq!(selected.fields()[1].name(), "a");

        let err = schema.select(&["nonexistent"]);
        assert!(err.is_err());
    }
}

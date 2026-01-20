//! In-memory data source implementation.

use dbms_dtype::{RecordBatch, Schema};

use crate::DataSource;

/// A data source that stores data in memory.
#[derive(Debug, Clone)]
pub struct InMemoryDataSource {
    schema: Schema,
    batches: Vec<RecordBatch>,
}

impl InMemoryDataSource {
    pub fn new(schema: Schema, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

impl DataSource for InMemoryDataSource {
    fn schema(&self) -> Result<Schema, String> {
        Ok(self.schema.clone())
    }

    fn scan(
        &self,
        projection: Option<&[&str]>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, String>>>, String> {
        let batches = match projection {
            None => self.batches.clone(),
            Some(cols) => {
                let indices: Vec<usize> = cols
                    .iter()
                    .map(|name| {
                        self.schema
                            .fields()
                            .iter()
                            .position(|f| f.name() == *name)
                            .ok_or_else(|| format!("column not found: {}", name))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let projected_schema = self.schema.project(&indices);

                self.batches
                    .iter()
                    .map(|batch| {
                        let projected_columns: Vec<_> =
                            indices.iter().map(|&i| batch.field(i).clone()).collect();
                        RecordBatch::new(projected_schema.clone(), projected_columns)
                    })
                    .collect()
            }
        };

        Ok(Box::new(batches.into_iter().map(Ok)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbms_dtype::{Column, DataType, Field, Scalar};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("name", DataType::Utf8),
            Field::new("age", DataType::Int64),
        ])
    }

    fn test_batches() -> Vec<RecordBatch> {
        let schema = test_schema();
        vec![RecordBatch::new(
            schema,
            vec![
                Column::from_literal(Scalar::Int64(Some(1)), 3),
                Column::from_literal(Scalar::Utf8(Some("Alice".to_string())), 3),
                Column::from_literal(Scalar::Int64(Some(30)), 3),
            ],
        )]
    }

    #[test]
    fn test_schema_retrieval() {
        let source = InMemoryDataSource::new(test_schema(), test_batches());
        let schema = source.schema().unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.fields()[0].name(), "id");
        assert_eq!(schema.fields()[1].name(), "name");
        assert_eq!(schema.fields()[2].name(), "age");
    }

    #[test]
    fn test_scan() {
        let source = InMemoryDataSource::new(test_schema(), test_batches());
        let batches: Vec<_> = source.scan(None).unwrap().collect();

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().unwrap();
        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.column_count(), 3);
    }

    #[test]
    fn test_projection() {
        let source = InMemoryDataSource::new(test_schema(), test_batches());
        let batches: Vec<_> = source.scan(Some(&["name", "age"])).unwrap().collect();

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().unwrap();
        assert_eq!(batch.column_count(), 2);
        assert_eq!(batch.schema().fields()[0].name(), "name");
        assert_eq!(batch.schema().fields()[1].name(), "age");
    }
}

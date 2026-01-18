//! Field type for the DBMS query engine.

use crate::DataType;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

/// A field in a schema, consisting of a name and data type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    dtype: DataType,
}

impl Field {
    pub fn new(name: impl Into<String>, dtype: DataType) -> Self {
        Self {
            name: name.into(),
            dtype,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }
}

impl From<Field> for ArrowField {
    fn from(f: Field) -> Self {
        ArrowField::new(f.name, ArrowDataType::from(f.dtype), true)
    }
}

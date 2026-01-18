//! Data types for the DBMS query engine.

mod column;
mod field;
mod schema;

pub use column::{Column, Scalar};
pub use field::Field;
pub use schema::Schema;

use arrow::datatypes::DataType as ArrowDataType;

/// Supported data types, a subset of Arrow's type system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
}

impl From<DataType> for ArrowDataType {
    fn from(dt: DataType) -> Self {
        match dt {
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Utf8 => ArrowDataType::Utf8,
            DataType::Binary => ArrowDataType::Binary,
        }
    }
}

impl TryFrom<ArrowDataType> for DataType {
    type Error = String;

    fn try_from(dt: ArrowDataType) -> Result<Self, Self::Error> {
        match dt {
            ArrowDataType::Boolean => Ok(DataType::Boolean),
            ArrowDataType::Int8 => Ok(DataType::Int8),
            ArrowDataType::Int16 => Ok(DataType::Int16),
            ArrowDataType::Int32 => Ok(DataType::Int32),
            ArrowDataType::Int64 => Ok(DataType::Int64),
            ArrowDataType::UInt8 => Ok(DataType::UInt8),
            ArrowDataType::UInt16 => Ok(DataType::UInt16),
            ArrowDataType::UInt32 => Ok(DataType::UInt32),
            ArrowDataType::UInt64 => Ok(DataType::UInt64),
            ArrowDataType::Float32 => Ok(DataType::Float32),
            ArrowDataType::Float64 => Ok(DataType::Float64),
            ArrowDataType::Utf8 => Ok(DataType::Utf8),
            ArrowDataType::Binary => Ok(DataType::Binary),
            other => Err(format!("unsupported Arrow type: {:?}", other)),
        }
    }
}

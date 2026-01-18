//! Column types for the DBMS query engine.

use arrow::array::ArrayRef;

use crate::DataType;

/// A scalar value that can be broadcast across rows.
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    Binary(Option<Vec<u8>>),
}

impl Scalar {
    /// Returns the data type of this scalar.
    pub fn dtype(&self) -> DataType {
        match self {
            Self::Boolean(_) => DataType::Boolean,
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt8(_) => DataType::UInt8,
            Self::UInt16(_) => DataType::UInt16,
            Self::UInt32(_) => DataType::UInt32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Binary(_) => DataType::Binary,
        }
    }

    /// Returns true if this scalar is null.
    pub fn is_null(&self) -> bool {
        match self {
            Self::Boolean(v) => v.is_none(),
            Self::Int8(v) => v.is_none(),
            Self::Int16(v) => v.is_none(),
            Self::Int32(v) => v.is_none(),
            Self::Int64(v) => v.is_none(),
            Self::UInt8(v) => v.is_none(),
            Self::UInt16(v) => v.is_none(),
            Self::UInt32(v) => v.is_none(),
            Self::UInt64(v) => v.is_none(),
            Self::Float32(v) => v.is_none(),
            Self::Float64(v) => v.is_none(),
            Self::Utf8(v) => v.is_none(),
            Self::Binary(v) => v.is_none(),
        }
    }
}

/// A column of data, either materialized as an Arrow array or a literal value.
#[derive(Debug, Clone)]
pub enum Column {
    /// A materialized Arrow array.
    Array(ArrayRef),
    /// A scalar value broadcast to a given length.
    Literal { value: Scalar, len: usize },
}

impl From<ArrayRef> for Column {
    fn from(array: ArrayRef) -> Self {
        Self::Array(array)
    }
}

impl Column {
    /// Creates a new literal column.
    pub fn from_literal(value: Scalar, len: usize) -> Self {
        Self::Literal { value, len }
    }

    /// Returns the number of rows in this column.
    pub fn len(&self) -> usize {
        match self {
            Self::Array(arr) => arr.len(),
            Self::Literal { len, .. } => *len,
        }
    }

    /// Returns true if this column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the data type of this column.
    pub fn dtype(&self) -> DataType {
        match self {
            Self::Array(arr) => arr
                .data_type()
                .clone()
                .try_into()
                .unwrap_or(DataType::Binary),
            Self::Literal { value, .. } => value.dtype(),
        }
    }
}

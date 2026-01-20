//! Column types for the DBMS query engine.

use crate::DataType;
use arrow::array::{
    ArrayRef, AsArray,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};

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
    /// Creates a null Scalar of the given data type.
    pub fn null(dtype: DataType) -> Self {
        match dtype {
            DataType::Boolean => Self::Boolean(None),
            DataType::Int8 => Self::Int8(None),
            DataType::Int16 => Self::Int16(None),
            DataType::Int32 => Self::Int32(None),
            DataType::Int64 => Self::Int64(None),
            DataType::UInt8 => Self::UInt8(None),
            DataType::UInt16 => Self::UInt16(None),
            DataType::UInt32 => Self::UInt32(None),
            DataType::UInt64 => Self::UInt64(None),
            DataType::Float32 => Self::Float32(None),
            DataType::Float64 => Self::Float64(None),
            DataType::Utf8 => Self::Utf8(None),
            DataType::Binary => Self::Binary(None),
        }
    }

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
}

/// A column of data, either materialized as an Arrow array or a literal value.
#[derive(Debug, Clone)]
pub enum Column {
    /// A materialized Arrow array.
    Array(ArrayRef),
    /// A scalar value broadcast to a given length.
    Literal { value: Scalar, len: usize },
}

impl TryFrom<ArrayRef> for Column {
    type Error = String;

    fn try_from(array: ArrayRef) -> Result<Self, Self::Error> {
        DataType::try_from(array.data_type().clone())?;
        Ok(Self::Array(array))
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
    ///
    /// # Panics
    /// Panics if the array has an unsupported Arrow type
    pub fn dtype(&self) -> DataType {
        match self {
            Self::Array(arr) => arr
                .data_type()
                .clone()
                .try_into()
                .unwrap_or_else(|_| panic!("unsupported Arrow type: {:?}", arr.data_type())),
            Self::Literal { value, .. } => value.dtype(),
        }
    }

    /// Returns the value at the given index as a Scalar.
    ///
    /// # Panics
    /// Panics if the index is out of bounds.
    pub fn get(&self, i: usize) -> Scalar {
        match self {
            Self::Literal { value, len } => {
                assert!(i < *len, "index out of bounds: {} >= {}", i, len);
                value.clone()
            }
            Self::Array(arr) => {
                if arr.is_null(i) {
                    return Scalar::null(self.dtype());
                }
                match self.dtype() {
                    DataType::Boolean => Scalar::Boolean(Some(arr.as_boolean().value(i))),
                    DataType::Int8 => Scalar::Int8(Some(arr.as_primitive::<Int8Type>().value(i))),
                    DataType::Int16 => {
                        Scalar::Int16(Some(arr.as_primitive::<Int16Type>().value(i)))
                    }
                    DataType::Int32 => {
                        Scalar::Int32(Some(arr.as_primitive::<Int32Type>().value(i)))
                    }
                    DataType::Int64 => {
                        Scalar::Int64(Some(arr.as_primitive::<Int64Type>().value(i)))
                    }
                    DataType::UInt8 => {
                        Scalar::UInt8(Some(arr.as_primitive::<UInt8Type>().value(i)))
                    }
                    DataType::UInt16 => {
                        Scalar::UInt16(Some(arr.as_primitive::<UInt16Type>().value(i)))
                    }
                    DataType::UInt32 => {
                        Scalar::UInt32(Some(arr.as_primitive::<UInt32Type>().value(i)))
                    }
                    DataType::UInt64 => {
                        Scalar::UInt64(Some(arr.as_primitive::<UInt64Type>().value(i)))
                    }
                    DataType::Float32 => {
                        Scalar::Float32(Some(arr.as_primitive::<Float32Type>().value(i)))
                    }
                    DataType::Float64 => {
                        Scalar::Float64(Some(arr.as_primitive::<Float64Type>().value(i)))
                    }
                    DataType::Utf8 => {
                        Scalar::Utf8(Some(arr.as_string::<i32>().value(i).to_string()))
                    }
                    DataType::Binary => {
                        Scalar::Binary(Some(arr.as_binary::<i32>().value(i).to_vec()))
                    }
                }
            }
        }
    }
}

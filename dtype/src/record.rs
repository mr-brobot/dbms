//! Record batch type for the DBMS query engine.

use arrow::record_batch::RecordBatch as ArrowRecordBatch;

use crate::{Column, Schema};

/// A batch of columnar data with a schema.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    schema: Schema,
    columns: Vec<Column>,
}

impl RecordBatch {
    pub fn new(schema: Schema, columns: Vec<Column>) -> Self {
        Self { schema, columns }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the column at the given index.
    pub fn field(&self, i: usize) -> &Column {
        &self.columns[i]
    }

    /// Returns the number of rows in this batch.
    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |c| c.len())
    }

    /// Returns the number of columns in this batch.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

impl TryFrom<ArrowRecordBatch> for RecordBatch {
    type Error = String;

    fn try_from(batch: ArrowRecordBatch) -> Result<Self, Self::Error> {
        let schema = Schema::try_from(batch.schema().as_ref())?;
        let columns: Result<Vec<Column>, String> = batch
            .columns()
            .iter()
            .map(|arr| Column::try_from(arr.clone()))
            .collect();
        Ok(Self::new(schema, columns?))
    }
}

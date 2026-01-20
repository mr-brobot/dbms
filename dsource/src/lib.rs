//! Data sources for the DBMS query engine.

mod csv;
mod memory;
mod parquet;

pub use csv::CsvDataSource;
pub use memory::InMemoryDataSource;
pub use parquet::ParquetDataSource;

use dbms_dtype::{RecordBatch, Schema};

/// A data source that can be scanned to produce record batches.
pub trait DataSource {
    /// Returns the schema of this data source.
    fn schema(&self) -> Result<Schema, String>;

    /// Scans the data source, optionally projecting to a subset of columns.
    fn scan(&self, projection: Option<&[&str]>) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, String>>>, String>;
}

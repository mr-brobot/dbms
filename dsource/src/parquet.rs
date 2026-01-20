//! Parquet data source implementation.

use std::fs::File;
use std::path::PathBuf;

use dbms_dtype::{RecordBatch, Schema};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::DataSource;

/// A data source that reads from Parquet files.
pub struct ParquetDataSource {
    path: PathBuf,
    batch_size: usize,
}

impl ParquetDataSource {
    pub fn new(path: impl Into<PathBuf>, batch_size: usize) -> Self {
        Self {
            path: path.into(),
            batch_size,
        }
    }
}

impl DataSource for ParquetDataSource {
    fn schema(&self) -> Result<Schema, String> {
        let file = File::open(&self.path).map_err(|e| e.to_string())?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
        Schema::try_from(builder.schema().as_ref())
    }

    fn scan(
        &self,
        projection: Option<&[&str]>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, String>>>, String> {
        let file = File::open(&self.path).map_err(|e| e.to_string())?;
        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
        let arrow_schema = builder.schema().clone();

        // Apply projection if specified
        if let Some(cols) = projection {
            let mask: Vec<bool> = arrow_schema
                .fields()
                .iter()
                .map(|f| cols.contains(&f.name().as_str()))
                .collect();
            let projection = parquet::arrow::ProjectionMask::leaves(
                builder.parquet_schema(),
                mask.iter()
                    .enumerate()
                    .filter_map(|(i, &b)| if b { Some(i) } else { None }),
            );
            builder = builder.with_projection(projection);
        }

        let reader = builder
            .with_batch_size(self.batch_size)
            .build()
            .map_err(|e| e.to_string())?;

        Ok(Box::new(ParquetBatchIterator { reader }))
    }
}

struct ParquetBatchIterator {
    reader: ParquetRecordBatchReader,
}

impl Iterator for ParquetBatchIterator {
    type Item = Result<RecordBatch, String>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next() {
            Some(Ok(batch)) => Some(batch.try_into()),
            Some(Err(e)) => Some(Err(e.to_string())),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_data_path(relative: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test/data")
            .join(relative)
    }

    #[test]
    fn test_schema() {
        let path = test_data_path("parquet/simple.parquet");
        let source = ParquetDataSource::new(path, 1024);
        let schema = source.schema().unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.fields()[0].name(), "id");
        assert_eq!(schema.fields()[1].name(), "name");
        assert_eq!(schema.fields()[2].name(), "age");
    }

    #[test]
    fn test_scan() {
        let path = test_data_path("parquet/simple.parquet");
        let source = ParquetDataSource::new(path, 1024);
        let batches: Vec<_> = source.scan(None).unwrap().collect();

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().unwrap();
        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.column_count(), 3);
    }

    #[test]
    fn test_projection() {
        let path = test_data_path("parquet/simple.parquet");
        let source = ParquetDataSource::new(path, 1024);
        let batches: Vec<_> = source.scan(Some(&["name", "age"])).unwrap().collect();

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().unwrap();
        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.column_count(), 2);
    }
}

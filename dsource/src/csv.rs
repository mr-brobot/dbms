//! CSV data source implementation.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::csv::reader::{Format, Reader, ReaderBuilder};
use arrow::datatypes::Schema as ArrowSchema;
use dbms_dtype::{RecordBatch, Schema};

use crate::DataSource;

/// A data source that reads from CSV files.
pub struct CsvDataSource {
    path: PathBuf,
    schema: Option<Schema>,
    batch_size: usize,
}

impl CsvDataSource {
    pub fn new(path: impl Into<PathBuf>, schema: Option<Schema>, batch_size: usize) -> Self {
        Self {
            path: path.into(),
            schema,
            batch_size,
        }
    }
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> Result<Schema, String> {
        if let Some(schema) = &self.schema {
            return Ok(schema.clone());
        }

        // Infer schema from file
        let file = File::open(&self.path).map_err(|e| e.to_string())?;
vscode ➜ /workspaces/dbmsile::open(&self.path).map_err(|e| e.to_string())?;
            .with_header(true)
            .infer_schema(&file, None)
            .map_err(|e| e.to_string())?;

        Schema::try_from(&arrow_schema)
    }

    fn scan(
        &self,
        projection: Option<&[&str]>,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, String>>>, String> {
        let schema = self.schema()?;
        let arrow_schema: ArrowSchema = schema.clone().into();

        let file = File::open(&self.path).map_err(|e| e.to_string())?;

        let mut builder = ReaderBuilder::new(Arc::new(arrow_schema))
            .with_header(true)
            .with_batch_size(self.batch_size);

        // Apply projection if specified
        if let Some(cols) = projection {
            let indices: Vec<usize> = cols
                .iter()
                .map(|name| {
                    schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == *name)
                        .ok_or_else(|| format!("column not found: {}", name))
                })
                .collect::<Result<Vec<_>, _>>()?;
            builder = builder.with_projection(indices.into());
        }

        let reader = builder.build(file).map_err(|e| e.to_string())?;

        Ok(Box::new(CsvBatchIterator { reader }))
    }
}

struct CsvBatchIterator {
    reader: Reader<File>,
}

impl Iterator for CsvBatchIterator {
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
    fn test_schema_inference() {
        let path = test_data_path("csv/simple.csv");
        let source = CsvDataSource::new(path, None, 1024);
        let schema = source.schema().unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.fields()[0].name(), "id");
        assert_eq!(schema.fields()[1].name(), "name");
        assert_eq!(schema.fields()[2].name(), "age");
    }

    #[test]
    fn test_scan() {
        let path = test_data_path("csv/simple.csv");
        let source = CsvDataSource::new(path, None, 1024);
        let batches: Vec<_> = source.scan(None).unwrap().collect();

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().unwrap();
        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.column_count(), 3);
    }

    #[test]
    fn test_projection() {
        let path = test_data_path("csv/simple.csv");
        let source = CsvDataSource::new(path, None, 1024);
        let batches: Vec<_> = source.scan(Some(&["name", "age"])).unwrap().collect();

        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().unwrap();
        assert_eq!(batch.column_count(), 2);
        assert_eq!(batch.schema().fields()[0].name(), "name");
        assert_eq!(batch.schema().fields()[1].name(), "age");
    }
}

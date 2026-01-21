//! Parquet writer for Hail tables
//!
//! This module provides `ParquetWriter`, which converts Hail row data to Parquet format.
//! It accumulates rows into batches and writes them incrementally to a Parquet file.

use crate::codec::{EncodedType, EncodedValue};
use crate::error::{HailError, Result};
use crate::parquet::builder::ColumnBuilder;
use crate::parquet::schema::create_schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;

/// A writer that converts Hail rows to Parquet format
///
/// The writer accumulates rows in memory up to a configurable batch size,
/// then flushes them to the Parquet file. This provides a balance between
/// memory usage and I/O efficiency.
pub struct ParquetWriter {
    /// The underlying Arrow/Parquet writer
    writer: ArrowWriter<File>,
    /// Column builders for each top-level field
    builders: Vec<ColumnBuilder>,
    /// The Arrow schema
    schema: Arc<arrow::datatypes::Schema>,
    /// Number of rows to accumulate before flushing
    batch_size: usize,
    /// Current number of accumulated rows
    current_count: usize,
    /// Total rows written
    total_rows: usize,
}

impl ParquetWriter {
    /// Create a new ParquetWriter
    ///
    /// # Arguments
    /// * `path` - Output path for the Parquet file
    /// * `hail_schema` - The EncodedType describing the row structure (must be a struct)
    ///
    /// # Example
    /// ```no_run
    /// use hail_decoder::codec::EncodedType;
    /// use hail_decoder::parquet::ParquetWriter;
    ///
    /// let schema = EncodedType::EBaseStruct {
    ///     required: true,
    ///     fields: vec![],
    /// };
    /// let writer = ParquetWriter::new("output.parquet", &schema).unwrap();
    /// ```
    pub fn new(path: &str, hail_schema: &EncodedType) -> Result<Self> {
        Self::with_batch_size(path, hail_schema, 4096)
    }

    /// Create a new ParquetWriter with a custom batch size
    ///
    /// # Arguments
    /// * `path` - Output path for the Parquet file
    /// * `hail_schema` - The EncodedType describing the row structure
    /// * `batch_size` - Number of rows to accumulate before flushing
    pub fn with_batch_size(path: &str, hail_schema: &EncodedType, batch_size: usize) -> Result<Self> {
        let file = File::create(path)?;
        let arrow_schema = Arc::new(create_schema(hail_schema)?);

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();

        let writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;

        // Initialize builders for each top-level field
        let builders = if let EncodedType::EBaseStruct { fields, .. } = hail_schema {
            fields
                .iter()
                .map(|f| ColumnBuilder::new(&f.encoded_type))
                .collect::<Result<Vec<_>>>()?
        } else {
            return Err(HailError::InvalidFormat(
                "Root schema must be a struct".to_string(),
            ));
        };

        Ok(Self {
            writer,
            builders,
            schema: arrow_schema,
            batch_size,
            current_count: 0,
            total_rows: 0,
        })
    }

    /// Write a single row to the Parquet file
    ///
    /// The row is buffered until `batch_size` rows are accumulated,
    /// at which point they are flushed to disk.
    ///
    /// # Arguments
    /// * `row` - The row value (must be a Struct)
    pub fn write_row(&mut self, row: &EncodedValue) -> Result<()> {
        if let EncodedValue::Struct(fields) = row {
            // Append each field value to its corresponding builder
            for (i, builder) in self.builders.iter_mut().enumerate() {
                if i < fields.len() {
                    builder.append(&fields[i].1)?;
                } else {
                    builder.append_null()?;
                }
            }

            self.current_count += 1;
            self.total_rows += 1;

            if self.current_count >= self.batch_size {
                self.flush()?;
            }

            Ok(())
        } else {
            Err(HailError::TypeMismatch {
                expected: "Struct".to_string(),
                actual: format!("{:?}", row),
            })
        }
    }

    /// Flush accumulated rows to the Parquet file
    ///
    /// This is called automatically when `batch_size` rows are accumulated,
    /// but can also be called manually.
    pub fn flush(&mut self) -> Result<()> {
        if self.current_count == 0 {
            return Ok(());
        }

        // Finish all builders and collect arrays
        let arrays: Vec<_> = self.builders.iter_mut().map(|b| b.finish()).collect();

        // Create a RecordBatch
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        // Write the batch
        self.writer.write(&batch)?;

        // Reinitialize builders for the next batch
        // We need to create new builders since finish() consumes them
        let fields = match &self.schema.fields()[..] {
            fields => fields,
        };

        self.builders = fields
            .iter()
            .map(|f| {
                let etype = arrow_type_to_encoded(f.data_type(), !f.is_nullable());
                ColumnBuilder::new(&etype)
            })
            .collect::<Result<Vec<_>>>()?;

        self.current_count = 0;
        Ok(())
    }

    /// Close the writer and finalize the Parquet file
    ///
    /// This flushes any remaining rows and writes the Parquet footer.
    /// Returns the total number of rows written.
    pub fn close(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.total_rows)
    }

    /// Get the total number of rows written so far
    pub fn rows_written(&self) -> usize {
        self.total_rows
    }

    /// Write a pre-built RecordBatch to the Parquet file
    ///
    /// This is useful for parallel conversion where batches are built
    /// in separate threads and then written sequentially.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        self.total_rows += batch.num_rows();
        Ok(())
    }

    /// Get the Arrow schema for this writer
    pub fn schema(&self) -> &Arc<arrow::datatypes::Schema> {
        &self.schema
    }
}

/// Build a RecordBatch from a slice of EncodedValue rows
///
/// This function can be called in parallel for different partitions.
/// The resulting batches can then be written sequentially using
/// `ParquetWriter::write_batch`.
pub fn build_record_batch(
    rows: &[EncodedValue],
    hail_schema: &EncodedType,
    arrow_schema: Arc<arrow::datatypes::Schema>,
) -> Result<RecordBatch> {
    // Initialize builders for each top-level field
    let mut builders = if let EncodedType::EBaseStruct { fields, .. } = hail_schema {
        fields
            .iter()
            .map(|f| ColumnBuilder::new(&f.encoded_type))
            .collect::<Result<Vec<_>>>()?
    } else {
        return Err(HailError::InvalidFormat(
            "Root schema must be a struct".to_string(),
        ));
    };

    // Append all rows
    for row in rows {
        if let EncodedValue::Struct(fields) = row {
            for (i, builder) in builders.iter_mut().enumerate() {
                if i < fields.len() {
                    builder.append(&fields[i].1)?;
                } else {
                    builder.append_null()?;
                }
            }
        } else {
            return Err(HailError::TypeMismatch {
                expected: "Struct".to_string(),
                actual: format!("{:?}", row),
            });
        }
    }

    // Finish all builders and create RecordBatch
    let arrays: Vec<_> = builders.iter_mut().map(|b| b.finish()).collect();
    let batch = RecordBatch::try_new(arrow_schema, arrays)?;
    Ok(batch)
}

/// Convert an Arrow DataType back to an EncodedType
/// This is needed to reinitialize builders after flushing
fn arrow_type_to_encoded(dtype: &arrow::datatypes::DataType, required: bool) -> EncodedType {
    use arrow::datatypes::DataType;

    match dtype {
        DataType::Int32 => EncodedType::EInt32 { required },
        DataType::Int64 => EncodedType::EInt64 { required },
        DataType::Float32 => EncodedType::EFloat32 { required },
        DataType::Float64 => EncodedType::EFloat64 { required },
        DataType::Boolean => EncodedType::EBoolean { required },
        DataType::Utf8 | DataType::LargeUtf8 => EncodedType::EBinary { required },
        DataType::List(field) => EncodedType::EArray {
            required,
            element: Box::new(arrow_type_to_encoded(field.data_type(), !field.is_nullable())),
        },
        DataType::Struct(fields) => {
            let encoded_fields: Vec<_> = fields
                .iter()
                .enumerate()
                .map(|(i, f)| crate::codec::EncodedField {
                    name: f.name().clone(),
                    encoded_type: arrow_type_to_encoded(f.data_type(), !f.is_nullable()),
                    index: i,
                })
                .collect();
            EncodedType::EBaseStruct {
                required,
                fields: encoded_fields,
            }
        }
        _ => EncodedType::EBinary { required }, // Fallback for unknown types
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::EncodedField;
    use tempfile::NamedTempFile;

    #[test]
    fn test_writer_simple() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let schema = EncodedType::EBaseStruct {
            required: true,
            fields: vec![
                EncodedField {
                    name: "id".to_string(),
                    encoded_type: EncodedType::EInt32 { required: true },
                    index: 0,
                },
                EncodedField {
                    name: "name".to_string(),
                    encoded_type: EncodedType::EBinary { required: false },
                    index: 1,
                },
            ],
        };

        let mut writer = ParquetWriter::with_batch_size(path, &schema, 2).unwrap();

        // Write some rows
        writer
            .write_row(&EncodedValue::Struct(vec![
                ("id".to_string(), EncodedValue::Int32(1)),
                ("name".to_string(), EncodedValue::Binary(b"Alice".to_vec())),
            ]))
            .unwrap();

        writer
            .write_row(&EncodedValue::Struct(vec![
                ("id".to_string(), EncodedValue::Int32(2)),
                ("name".to_string(), EncodedValue::Binary(b"Bob".to_vec())),
            ]))
            .unwrap();

        writer
            .write_row(&EncodedValue::Struct(vec![
                ("id".to_string(), EncodedValue::Int32(3)),
                ("name".to_string(), EncodedValue::Null),
            ]))
            .unwrap();

        let rows = writer.close().unwrap();
        assert_eq!(rows, 3);

        // Verify the file was created and has content
        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);
    }
}

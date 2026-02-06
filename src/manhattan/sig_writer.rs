//! Parquet writer for significant hit rows during Manhattan scan phase.
//!
//! This writer is optimized for the flat schema used by `SigHitRow` and writes
//! directly to Parquet without going through Hail's EncodedType system.

use crate::error::Result;
use crate::manhattan::data::SigHitRow;
use arrow::array::{ArrayRef, Float64Builder, Int32Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

/// A writer for significant hit rows during the scan phase.
///
/// Buffers rows and writes them as Parquet with a flat schema:
/// - contig: string
/// - position: int32
/// - ref: string
/// - alt: string
/// - pvalue: float64
/// - beta: float64 (nullable)
/// - se: float64 (nullable)
/// - af: float64 (nullable)
pub struct SigHitWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    buffer: Vec<SigHitRow>,
    batch_size: usize,
    total_rows: usize,
}

impl SigHitWriter<File> {
    /// Create a new SigHitWriter writing to a local file.
    pub fn new(path: &str) -> Result<Self> {
        Self::with_batch_size(path, 4096)
    }

    /// Create a new SigHitWriter with a custom batch size.
    pub fn with_batch_size(path: &str, batch_size: usize) -> Result<Self> {
        let file = File::create(path)?;
        Self::from_writer_with_batch_size(file, batch_size)
    }
}

impl<W: Write + Send> SigHitWriter<W> {
    /// Create a SigHitWriter from any Write implementation.
    pub fn from_writer(writer: W) -> Result<Self> {
        Self::from_writer_with_batch_size(writer, 4096)
    }

    /// Create a SigHitWriter from a writer with a custom batch size.
    pub fn from_writer_with_batch_size(writer: W, batch_size: usize) -> Result<Self> {
        let schema = Arc::new(sig_hit_schema());

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .set_max_row_group_size(100_000)
            .build();

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            total_rows: 0,
        })
    }

    /// Write a significant hit row.
    pub fn write(&mut self, row: SigHitRow) -> Result<()> {
        self.buffer.push(row);

        if self.buffer.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush buffered rows to the Parquet file.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch = self.build_batch()?;
        self.writer.write(&batch)?;
        self.total_rows += self.buffer.len();
        self.buffer.clear();

        Ok(())
    }

    /// Close the writer and finalize the Parquet file.
    pub fn finish(mut self) -> Result<usize> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.total_rows)
    }

    /// Finish writing and return the underlying writer.
    ///
    /// This allows accessing the writer after Parquet finalization, which is
    /// necessary for CloudWriter to trigger the upload.
    pub fn into_inner(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.writer.into_inner()?)
    }

    /// Get the number of rows written so far.
    pub fn rows_written(&self) -> usize {
        self.total_rows + self.buffer.len()
    }

    /// Build a RecordBatch from the buffered rows.
    fn build_batch(&self) -> Result<RecordBatch> {
        let mut contig_builder = StringBuilder::new();
        let mut position_builder = Int32Builder::new();
        let mut ref_builder = StringBuilder::new();
        let mut alt_builder = StringBuilder::new();
        let mut pvalue_builder = Float64Builder::new();
        let mut beta_builder = Float64Builder::new();
        let mut se_builder = Float64Builder::new();
        let mut af_builder = Float64Builder::new();

        for row in &self.buffer {
            contig_builder.append_value(&row.contig);
            position_builder.append_value(row.position);
            ref_builder.append_value(&row.ref_allele);
            alt_builder.append_value(&row.alt_allele);
            pvalue_builder.append_value(row.pvalue);

            match row.beta {
                Some(v) => beta_builder.append_value(v),
                None => beta_builder.append_null(),
            }
            match row.se {
                Some(v) => se_builder.append_value(v),
                None => se_builder.append_null(),
            }
            match row.af {
                Some(v) => af_builder.append_value(v),
                None => af_builder.append_null(),
            }
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(contig_builder.finish()),
            Arc::new(position_builder.finish()),
            Arc::new(ref_builder.finish()),
            Arc::new(alt_builder.finish()),
            Arc::new(pvalue_builder.finish()),
            Arc::new(beta_builder.finish()),
            Arc::new(se_builder.finish()),
            Arc::new(af_builder.finish()),
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(batch)
    }
}

/// Create the Arrow schema for significant hit rows.
pub fn sig_hit_schema() -> Schema {
    Schema::new(vec![
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("pvalue", DataType::Float64, false),
        Field::new("beta", DataType::Float64, true),
        Field::new("se", DataType::Float64, true),
        Field::new("af", DataType::Float64, true),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sig_hit_writer() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut writer = SigHitWriter::with_batch_size(path, 2).unwrap();

        writer
            .write(SigHitRow {
                contig: "1".to_string(),
                position: 12345,
                ref_allele: "A".to_string(),
                alt_allele: "G".to_string(),
                pvalue: 1e-10,
                beta: Some(0.5),
                se: Some(0.1),
                af: Some(0.01),
            })
            .unwrap();

        writer
            .write(SigHitRow {
                contig: "2".to_string(),
                position: 67890,
                ref_allele: "C".to_string(),
                alt_allele: "T".to_string(),
                pvalue: 5e-9,
                beta: None,
                se: None,
                af: None,
            })
            .unwrap();

        let rows = writer.finish().unwrap();
        assert_eq!(rows, 2);

        // Verify file was created
        let metadata = std::fs::metadata(path).unwrap();
        assert!(metadata.len() > 0);
    }
}

//! Hail to Parquet conversion utilities
//!
//! This module provides a reusable function for converting Hail tables to Parquet format.

use crate::parquet::{build_record_batch, ParquetWriter};
use crate::query::QueryEngine;
use crate::Result;
use arrow::record_batch::RecordBatch;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;

/// Convert a Hail table to Parquet format.
///
/// # Arguments
///
/// * `input_path` - Path to the input Hail table (local or cloud)
/// * `output_path` - Path for the output Parquet file
///
/// # Returns
///
/// The total number of rows written to the Parquet file.
///
/// # Example
///
/// ```rust,no_run
/// use hail_decoder::parquet::hail_to_parquet;
///
/// let rows_written = hail_to_parquet("input.ht", "output.parquet")?;
/// println!("Wrote {} rows", rows_written);
/// # Ok::<(), hail_decoder::HailError>(())
/// ```
pub fn hail_to_parquet(input_path: &str, output_path: &str) -> Result<usize> {
    hail_to_parquet_with_progress(input_path, output_path, true)
}

/// Convert a Hail table to Parquet format with optional progress output.
///
/// # Arguments
///
/// * `input_path` - Path to the input Hail table (local or cloud)
/// * `output_path` - Path for the output Parquet file
/// * `show_progress` - Whether to display progress bar
///
/// # Returns
///
/// The total number of rows written to the Parquet file.
pub fn hail_to_parquet_with_progress(
    input_path: &str,
    output_path: &str,
    show_progress: bool,
) -> Result<usize> {
    hail_to_parquet_with_options(input_path, output_path, show_progress, None)
}

/// Options for Hail to Parquet conversion
pub struct ConversionOptions {
    /// Maximum number of partitions to convert (None = all)
    pub max_partitions: Option<usize>,
    /// Whether to show progress bar
    pub show_progress: bool,
}

impl Default for ConversionOptions {
    fn default() -> Self {
        Self {
            max_partitions: None,
            show_progress: true,
        }
    }
}

/// Convert a Hail table to Parquet format with full options.
///
/// # Arguments
///
/// * `input_path` - Path to the input Hail table (local or cloud)
/// * `output_path` - Path for the output Parquet file
/// * `show_progress` - Whether to display progress bar
/// * `max_partitions` - Maximum number of partitions to convert (None = all)
///
/// # Returns
///
/// The total number of rows written to the Parquet file.
pub fn hail_to_parquet_with_options(
    input_path: &str,
    output_path: &str,
    show_progress: bool,
    max_partitions: Option<usize>,
) -> Result<usize> {
    // Open the query engine to read the table
    let engine = QueryEngine::open_path(input_path)?;
    let total_partitions = engine.num_partitions();
    let num_partitions = max_partitions.map(|m| m.min(total_partitions)).unwrap_or(total_partitions);
    let row_type = engine.row_type().clone();

    // Create the Parquet writer to get the schema
    let mut writer = ParquetWriter::new(output_path, &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Setup progress bar if requested
    let pb = if show_progress {
        let pb = ProgressBar::new(num_partitions as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} partitions ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    // Parallel scan: each partition produces a RecordBatch
    let batches: Vec<Result<RecordBatch>> = (0..num_partitions)
        .into_par_iter()
        .map(|i| {
            let rows = engine.scan_partition(i, &[])?;
            let batch = build_record_batch(&rows, &row_type, arrow_schema.clone())?;
            if let Some(ref pb) = pb {
                pb.inc(1);
            }
            Ok(batch)
        })
        .collect();

    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    // Write all batches sequentially (each becomes a row group)
    let mut total_rows = 0;
    for batch_result in batches {
        let batch = batch_result?;
        total_rows += batch.num_rows();
        writer.write_batch(&batch)?;
    }

    // Close the writer
    writer.close()?;

    Ok(total_rows)
}

/// Get metadata about the conversion without performing it.
///
/// Useful for showing table info before conversion or for ClickHouse schema generation.
pub struct ConversionMetadata {
    pub num_partitions: usize,
    pub key_fields: Vec<String>,
    pub row_type: crate::codec::EncodedType,
}

impl ConversionMetadata {
    /// Gather metadata from a Hail table.
    pub fn from_path(input_path: &str) -> Result<Self> {
        let engine = QueryEngine::open_path(input_path)?;
        Ok(Self {
            num_partitions: engine.num_partitions(),
            key_fields: engine.key_fields().to_vec(),
            row_type: engine.row_type().clone(),
        })
    }
}

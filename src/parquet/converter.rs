//! Hail to Parquet conversion utilities
//!
//! This module provides a reusable function for converting Hail tables to Parquet format.

use crate::parquet::{build_record_batch, ParquetWriter};
use crate::query::QueryEngine;
use crate::Result;
use arrow::record_batch::RecordBatch;
use crossbeam_channel;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::thread;

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
/// Uses a producer-consumer pattern with bounded channels for efficient parallelization.
/// Worker threads process partitions in parallel and send batches to a dedicated writer thread.
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
    let num_partitions = max_partitions
        .map(|m| m.min(total_partitions))
        .unwrap_or(total_partitions);
    let row_type = engine.row_type().clone();

    // Create the Parquet writer to get the schema, then move to writer thread
    let writer = ParquetWriter::new(output_path, &row_type)?;
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

    // Create a bounded channel - capacity 32 keeps writer busy without bloating memory
    let (tx, rx) = crossbeam_channel::bounded::<Result<RecordBatch>>(32);

    // Spawn the writer thread
    let writer_handle = thread::spawn(move || -> Result<usize> {
        let mut writer = writer;
        let mut total_rows = 0;

        // Receive and write batches from workers
        for batch_result in rx {
            let batch = batch_result?;
            total_rows += batch.num_rows();
            writer.write_batch(&batch)?;
        }

        // Close writer when all senders drop
        writer.close()?;
        Ok(total_rows)
    });

    // Run parallel workers using try_for_each_with
    // This clones the sender per-thread, enabling true parallelism
    let engine_ref = &engine;
    let row_type_ref = &row_type;
    let schema_ref = &arrow_schema;

    // Batch size for streaming - accumulate rows before building Arrow batch
    const BATCH_SIZE: usize = 4096;

    let worker_result: Result<()> = (0..num_partitions)
        .into_par_iter()
        .try_for_each_with(tx, |sender, i| -> Result<()> {
            // Stream partition rows instead of loading all into memory
            let iter = engine_ref.scan_partition_iter(i, &[])?;

            // Accumulate rows in batches to avoid OOM
            let mut batch_rows = Vec::with_capacity(BATCH_SIZE);

            for row_result in iter {
                let row = row_result?;
                batch_rows.push(row);

                if batch_rows.len() >= BATCH_SIZE {
                    // Build and send Arrow batch
                    let batch = build_record_batch(&batch_rows, row_type_ref, schema_ref.clone())?;

                    if sender.send(Ok(batch)).is_err() {
                        return Err(crate::HailError::Io(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "Writer thread disconnected",
                        )));
                    }

                    batch_rows.clear();
                }
            }

            // Send remaining rows
            if !batch_rows.is_empty() {
                let batch = build_record_batch(&batch_rows, row_type_ref, schema_ref.clone())?;
                if sender.send(Ok(batch)).is_err() {
                    return Err(crate::HailError::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "Writer thread disconnected",
                    )));
                }
            }

            // Update progress after partition complete
            if let Some(ref pb) = pb {
                pb.inc(1);
            }

            Ok(())
        });

    if let Some(pb) = &pb {
        pb.finish_and_clear();
    }

    // Handle results from workers and writer
    match worker_result {
        Ok(()) => {
            // Workers finished successfully, wait for writer
            match writer_handle.join() {
                Ok(writer_res) => writer_res,
                Err(e) => std::panic::resume_unwind(e),
            }
        }
        Err(e) => {
            // Workers failed. Wait for writer to clean up, then return worker error.
            let _ = writer_handle.join();
            Err(e)
        }
    }
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

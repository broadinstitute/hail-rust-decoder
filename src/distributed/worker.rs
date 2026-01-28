//! Worker client for distributed processing.
//!
//! The worker connects to a coordinator, requests work, processes partitions,
//! and reports completion. It loops until receiving an Exit response.

use crate::distributed::message::{CompleteRequest, WorkRequest, WorkResponse};
use crate::io::{is_cloud_path, StreamingCloudWriter};
use crate::parquet::{build_record_batch, ParquetWriter};
use crate::query::QueryEngine;
use crate::Result;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for a worker.
pub struct WorkerConfig {
    /// URL of the coordinator (e.g., "http://10.0.0.5:3000")
    pub coordinator_url: String,
    /// Unique identifier for this worker
    pub worker_id: String,
    /// Retry delay when waiting for work (milliseconds)
    pub poll_interval_ms: u64,
    /// Connection timeout (seconds)
    pub connect_timeout_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            coordinator_url: String::new(),
            worker_id: String::new(),
            poll_interval_ms: 2000,
            connect_timeout_secs: 30,
        }
    }
}

/// Run the worker loop.
///
/// This function blocks until the coordinator signals job completion.
pub async fn run_worker(config: WorkerConfig) -> Result<()> {
    println!(
        "Worker {} starting, connecting to {}",
        config.worker_id, config.coordinator_url
    );

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
        .timeout(Duration::from_secs(300)) // 5 minute request timeout
        .build()
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create HTTP client: {}", e),
            ))
        })?;

    let work_url = format!("{}/work", config.coordinator_url);
    let complete_url = format!("{}/complete", config.coordinator_url);

    // Cache the QueryEngine between work requests to avoid re-reading metadata
    let mut cached_engine: Option<(String, QueryEngine)> = None;

    loop {
        // Request work from coordinator
        let work_response = match request_work(&client, &work_url, &config.worker_id).await {
            Ok(resp) => resp,
            Err(e) => {
                eprintln!(
                    "Failed to connect to coordinator: {}. Retrying in {}ms...",
                    e, config.poll_interval_ms
                );
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms * 2)).await;
                continue;
            }
        };

        match work_response {
            WorkResponse::Exit => {
                println!("Received Exit signal. Worker shutting down.");
                break;
            }
            WorkResponse::Wait => {
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
            }
            WorkResponse::Task {
                partitions,
                input_path,
                output_path,
                total_partitions: _,
            } => {
                println!(
                    "Received work: {} partition(s) {:?}",
                    partitions.len(),
                    partitions
                );

                // Process the assigned partitions on a blocking thread
                // (QueryEngine uses blocking I/O internally)
                // Pass cached engine in and get it back to avoid re-reading metadata
                let partitions_clone = partitions.clone();
                let input_clone = input_path.clone();
                let output_clone = output_path.clone();

                let result = tokio::task::spawn_blocking(move || {
                    process_partitions_sync(
                        cached_engine,
                        &partitions_clone,
                        &input_clone,
                        &output_clone,
                    )
                })
                .await;

                let rows_processed = match result {
                    Ok(Ok((rows, engine_back))) => {
                        cached_engine = engine_back;
                        rows
                    }
                    Ok(Err(e)) => {
                        eprintln!("Error processing partitions {:?}: {}", partitions, e);
                        cached_engine = None;
                        // Don't report completion - let coordinator reassign after timeout
                        continue;
                    }
                    Err(e) => {
                        eprintln!("Task panicked processing partitions {:?}: {}", partitions, e);
                        cached_engine = None;
                        continue;
                    }
                };

                // Report completion
                if let Err(e) = report_completion(
                    &client,
                    &complete_url,
                    &config.worker_id,
                    &partitions,
                    rows_processed,
                )
                .await
                {
                    eprintln!("Failed to report completion: {}", e);
                    // Continue anyway - coordinator will handle duplicates
                }

                println!(
                    "Completed partitions {:?} ({} rows)",
                    partitions, rows_processed
                );
            }
        }
    }

    Ok(())
}

/// Request work from the coordinator.
async fn request_work(
    client: &reqwest::Client,
    url: &str,
    worker_id: &str,
) -> Result<WorkResponse> {
    let request = WorkRequest {
        worker_id: worker_id.to_string(),
    };

    let response = client
        .post(url)
        .json(&request)
        .send()
        .await
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP request failed: {}", e),
            ))
        })?;

    let work_response: WorkResponse = response.json().await.map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to parse response: {}", e),
        ))
    })?;

    Ok(work_response)
}

/// Report completion to the coordinator.
async fn report_completion(
    client: &reqwest::Client,
    url: &str,
    worker_id: &str,
    partitions: &[usize],
    rows_processed: usize,
) -> Result<()> {
    let request = CompleteRequest {
        worker_id: worker_id.to_string(),
        partitions: partitions.to_vec(),
        rows_processed,
    };

    client
        .post(url)
        .json(&request)
        .send()
        .await
        .map_err(|e| {
            crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP request failed: {}", e),
            ))
        })?;

    Ok(())
}

/// Process the assigned partitions and write to output (synchronous version).
/// Accepts an optional cached engine and returns it for reuse across work requests.
fn process_partitions_sync(
    cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    output_path: &str,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    // Reuse cached engine if input path matches, otherwise open a new one
    let engine = match cached_engine {
        Some((path, engine)) if path == input_path => {
            println!("Reusing cached QueryEngine for {}", input_path);
            engine
        }
        _ => {
            println!("Opening input: {}", input_path);
            QueryEngine::open_path(input_path)?
        }
    };
    let row_type = engine.row_type().clone();

    // Create schema for Parquet output
    let arrow_schema = Arc::new(crate::parquet::schema::create_schema(&row_type)?);

    let output_is_cloud = is_cloud_path(output_path);
    let mut total_rows = 0;

    const BATCH_SIZE: usize = 4096;

    // Process each partition
    for &partition_id in partitions {
        // Determine the shard file name based on partition ID
        // In distributed mode, we write one file per partition
        let output_file = if output_is_cloud {
            let base = output_path.trim_end_matches('/');
            format!("{}/part-{:05}.parquet", base, partition_id)
        } else {
            format!("{}/part-{:05}.parquet", output_path, partition_id)
        };

        let mut batch_rows = Vec::with_capacity(BATCH_SIZE);
        let mut partition_rows = 0;

        // Stream rows from this partition
        let iter = engine.scan_partition_iter(partition_id, &[])?;

        // Helper macro for processing with any writer type
        macro_rules! process_with_writer {
            ($writer:expr) => {{
                let mut writer = $writer;

                for row_result in iter {
                    let row = row_result?;
                    batch_rows.push(row);

                    if batch_rows.len() >= BATCH_SIZE {
                        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                        writer.write_batch(&batch)?;
                        partition_rows += batch_rows.len();
                        batch_rows.clear();
                    }
                }

                // Write remaining rows
                if !batch_rows.is_empty() {
                    let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                    writer.write_batch(&batch)?;
                    partition_rows += batch_rows.len();
                }

                writer
            }};
        }

        if output_is_cloud {
            let cloud_writer = StreamingCloudWriter::new(&output_file)?;
            let writer = ParquetWriter::from_writer(cloud_writer, &row_type)?;
            let writer = process_with_writer!(writer);
            let cloud_writer = writer.into_inner()?;
            cloud_writer.finish()?;
        } else {
            let writer = ParquetWriter::new(&output_file, &row_type)?;
            let writer = process_with_writer!(writer);
            writer.close()?;
        }

        total_rows += partition_rows;
        println!(
            "  Partition {} complete: {} rows -> {}",
            partition_id, partition_rows, output_file
        );
    }

    Ok((total_rows, Some((input_path.to_string(), engine))))
}

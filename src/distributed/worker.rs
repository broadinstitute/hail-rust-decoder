//! Worker client for distributed processing.
//!
//! The worker connects to a coordinator, requests work, processes partitions,
//! and reports completion. It loops until receiving an Exit response.
//!
//! Includes a background telemetry loop that sends heartbeats with system
//! metrics to the coordinator for the dashboard UI.

use crate::distributed::message::{
    CompleteRequest, HeartbeatRequest, TelemetrySnapshot, WorkRequest, WorkResponse,
};
use crate::io::{is_cloud_path, StreamingCloudWriter};
use crate::parquet::{build_record_batch, ParquetWriter};
use crate::query::QueryEngine;
use crate::Result;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

/// Shared state between the main worker loop and the telemetry background task.
struct TelemetryState {
    /// Total rows processed so far
    total_rows: AtomicUsize,
    /// Currently active partition (usize::MAX = none)
    active_partition: AtomicUsize,
    /// Total partitions completed
    partitions_completed: AtomicUsize,
    /// Signal to stop the telemetry loop
    stop: AtomicBool,
}

const NO_ACTIVE_PARTITION: usize = usize::MAX;

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

    // Shared telemetry state between main loop and background heartbeat task
    let telemetry_state = Arc::new(TelemetryState {
        total_rows: AtomicUsize::new(0),
        active_partition: AtomicUsize::new(NO_ACTIVE_PARTITION),
        partitions_completed: AtomicUsize::new(0),
        stop: AtomicBool::new(false),
    });

    // Spawn background telemetry heartbeat loop
    let heartbeat_handle = spawn_telemetry_loop(
        client.clone(),
        config.coordinator_url.clone(),
        config.worker_id.clone(),
        telemetry_state.clone(),
    );

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
                telemetry_state
                    .active_partition
                    .store(NO_ACTIVE_PARTITION, Ordering::Relaxed);
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

                // Update telemetry: mark first partition as active
                if let Some(&first) = partitions.first() {
                    telemetry_state
                        .active_partition
                        .store(first, Ordering::Relaxed);
                }

                // Process the assigned partitions on a blocking thread
                // (QueryEngine uses blocking I/O internally)
                // Pass cached engine in and get it back to avoid re-reading metadata
                let partitions_clone = partitions.clone();
                let input_clone = input_path.clone();
                let output_clone = output_path.clone();
                let ts = telemetry_state.clone();

                let result = tokio::task::spawn_blocking(move || {
                    process_partitions_sync(
                        cached_engine,
                        &partitions_clone,
                        &input_clone,
                        &output_clone,
                        Some(&ts),
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

                // Update telemetry counters
                telemetry_state
                    .active_partition
                    .store(NO_ACTIVE_PARTITION, Ordering::Relaxed);
                telemetry_state
                    .partitions_completed
                    .fetch_add(partitions.len(), Ordering::Relaxed);

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

    // Stop telemetry background task
    telemetry_state.stop.store(true, Ordering::Relaxed);
    let _ = heartbeat_handle.await;

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

/// Spawn a background task that periodically sends heartbeats with telemetry to the coordinator.
fn spawn_telemetry_loop(
    client: reqwest::Client,
    coordinator_url: String,
    worker_id: String,
    state: Arc<TelemetryState>,
) -> tokio::task::JoinHandle<()> {
    let heartbeat_url = format!("{}/heartbeat", coordinator_url);
    let start_time = Instant::now();

    // Initialize sysinfo for system metrics (CPU, memory)
    let sys = std::sync::Mutex::new({
        use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
        let mut sys = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::new().with_cpu_usage())
                .with_memory(MemoryRefreshKind::new().with_ram()),
        );
        // Initial refresh to establish baseline
        sys.refresh_all();
        sys
    });

    let mut prev_rows: usize = 0;
    let mut prev_time = start_time;

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;

            if state.stop.load(Ordering::Relaxed) {
                break;
            }

            let total_rows = state.total_rows.load(Ordering::Relaxed);
            let active_part = state.active_partition.load(Ordering::Relaxed);
            let parts_done = state.partitions_completed.load(Ordering::Relaxed);

            let now = Instant::now();
            let dt = now.duration_since(prev_time).as_secs_f64();
            let rows_per_sec = if dt > 0.0 {
                (total_rows.saturating_sub(prev_rows)) as f64 / dt
            } else {
                0.0
            };
            prev_rows = total_rows;
            prev_time = now;

            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // Collect system metrics
            let (cpu, mem_used, mem_total) = {
                use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};
                let mut s = sys.lock().unwrap();
                s.refresh_specifics(
                    RefreshKind::new()
                        .with_cpu(CpuRefreshKind::new().with_cpu_usage())
                        .with_memory(MemoryRefreshKind::new().with_ram()),
                );
                let cpu_avg = s.cpus().iter().map(|c| c.cpu_usage()).sum::<f32>()
                    / s.cpus().len().max(1) as f32;
                (Some(cpu_avg), Some(s.used_memory()), Some(s.total_memory()))
            };

            let snapshot = TelemetrySnapshot {
                timestamp_ms,
                cpu_percent: cpu,
                memory_used_bytes: mem_used,
                memory_total_bytes: mem_total,
                rows_per_sec,
                total_rows,
                active_partition: if active_part == NO_ACTIVE_PARTITION {
                    None
                } else {
                    Some(active_part)
                },
                partitions_completed: parts_done,
            };

            let req = HeartbeatRequest {
                worker_id: worker_id.clone(),
                telemetry: snapshot,
            };

            // Best-effort: don't let heartbeat failures block the worker
            let _ = client.post(&heartbeat_url).json(&req).send().await;
        }
    })
}

/// Process the assigned partitions and write to output (synchronous version).
/// Accepts an optional cached engine and returns it for reuse across work requests.
fn process_partitions_sync(
    cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    output_path: &str,
    telemetry: Option<&TelemetryState>,
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
        // Update telemetry: mark this partition as active
        if let Some(ts) = telemetry {
            ts.active_partition
                .store(partition_id, Ordering::Relaxed);
        }

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
                        // Update telemetry row count
                        if let Some(ts) = telemetry {
                            ts.total_rows.fetch_add(batch_rows.len(), Ordering::Relaxed);
                        }
                        batch_rows.clear();
                    }
                }

                // Write remaining rows
                if !batch_rows.is_empty() {
                    let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                    writer.write_batch(&batch)?;
                    partition_rows += batch_rows.len();
                    // Update telemetry row count
                    if let Some(ts) = telemetry {
                        ts.total_rows.fetch_add(batch_rows.len(), Ordering::Relaxed);
                    }
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

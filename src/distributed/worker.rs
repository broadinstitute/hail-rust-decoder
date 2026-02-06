//! Worker client for distributed processing.
//!
//! The worker connects to a coordinator, requests work, processes partitions,
//! and reports completion. It loops until receiving an Exit response.
//!
//! Includes a background telemetry loop that sends heartbeats with system
//! metrics to the coordinator for the dashboard UI.

use crate::distributed::message::{
    CompleteRequest, HeartbeatRequest, JobSpec, ManhattanAggregateSpec, ManhattanScanSpec,
    ManhattanSource, ManhattanSpec, TelemetrySnapshot, WorkRequest, WorkResponse,
};
use crate::io::{is_cloud_path, StreamingCloudWriter};
use crate::parquet::{build_record_batch, ParquetWriter};
use crate::query::{IntervalList, KeyRange, QueryEngine};
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
                job_spec,
                total_partitions: _,
                filters,
                intervals,
            } => {
                println!(
                    "Received work: {} partition(s) {:?} ({})",
                    partitions.len(),
                    partitions,
                    job_spec.description()
                );

                // Update telemetry: mark first partition as active
                if let Some(&first) = partitions.first() {
                    telemetry_state
                        .active_partition
                        .store(first, Ordering::Relaxed);
                }

                // Process the assigned partitions on a blocking thread
                // (QueryEngine uses blocking I/O internally)
                let partitions_clone = partitions.clone();
                let input_clone = input_path.clone();
                let job_spec_clone = job_spec.clone();
                let filters_clone = filters.clone();
                let intervals_clone = intervals.clone();
                let ts = telemetry_state.clone();

                let result = tokio::task::spawn_blocking(move || {
                    dispatch_job(
                        cached_engine,
                        &partitions_clone,
                        &input_clone,
                        &job_spec_clone,
                        &filters_clone,
                        &intervals_clone,
                        Some(ts),
                    )
                })
                .await;

                let (rows_processed, result_json) = match result {
                    Ok(Ok((rows, result, engine_back))) => {
                        cached_engine = engine_back;
                        (rows, result)
                    }
                    Ok(Err(e)) => {
                        let error_msg = format!("{}", e);
                        eprintln!("Error processing partitions {:?}: {}", partitions, error_msg);
                        cached_engine = None;

                        // Report failure to coordinator so it can track and display the error
                        let fail_req = CompleteRequest {
                            worker_id: config.worker_id.clone(),
                            partitions: partitions.clone(),
                            rows_processed: 0,
                            result_json: None,
                            error: Some(error_msg),
                        };
                        if let Err(post_err) = client.post(&complete_url).json(&fail_req).send().await {
                            eprintln!("Failed to report error to coordinator: {}", post_err);
                        }
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

                // Report completion (with optional result_json for aggregation)
                if let Err(e) = report_completion(
                    &client,
                    &complete_url,
                    &config.worker_id,
                    &partitions,
                    rows_processed,
                    result_json,
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
    result_json: Option<serde_json::Value>,
) -> Result<()> {
    let request = CompleteRequest {
        worker_id: worker_id.to_string(),
        partitions: partitions.to_vec(),
        rows_processed,
        result_json,
        error: None,
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

    // Initialize disk and network monitoring
    let disks = std::sync::Mutex::new(sysinfo::Disks::new_with_refreshed_list());
    let networks = std::sync::Mutex::new(sysinfo::Networks::new_with_refreshed_list());

    let mut prev_rows: usize = 0;
    let mut prev_time = start_time;

    // Track previous network counters for rate calculation
    let mut prev_net_rx: u64 = 0;
    let mut prev_net_tx: u64 = 0;

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

            // Collect system metrics (CPU and memory)
            let (cpu, cpu_per_core, mem_used, mem_total) = {
                use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};
                let mut s = sys.lock().unwrap();
                s.refresh_specifics(
                    RefreshKind::new()
                        .with_cpu(CpuRefreshKind::new().with_cpu_usage())
                        .with_memory(MemoryRefreshKind::new().with_ram()),
                );
                let per_core: Vec<f32> = s.cpus().iter().map(|c| c.cpu_usage()).collect();
                let cpu_avg = per_core.iter().sum::<f32>() / per_core.len().max(1) as f32;
                (
                    Some(cpu_avg),
                    Some(per_core),
                    Some(s.used_memory()),
                    Some(s.total_memory()),
                )
            };

            // Collect disk metrics
            let (disk_used, disk_total) = {
                let mut d = disks.lock().unwrap();
                d.refresh_list();
                let mut used = 0u64;
                let mut total = 0u64;
                for disk in d.iter() {
                    total += disk.total_space();
                    used += disk.total_space().saturating_sub(disk.available_space());
                }
                (Some(used), Some(total))
            };

            // Collect network metrics
            let (net_rx_sec, net_tx_sec, net_rx_total, net_tx_total) = {
                let mut n = networks.lock().unwrap();
                n.refresh();
                let (current_rx, current_tx) = n
                    .iter()
                    .fold((0u64, 0u64), |(rx, tx), (_, iface)| {
                        (rx + iface.total_received(), tx + iface.total_transmitted())
                    });

                // Calculate rates (bytes/sec)
                let rx_sec = if dt > 0.0 {
                    current_rx.saturating_sub(prev_net_rx) as f64 / dt
                } else {
                    0.0
                };
                let tx_sec = if dt > 0.0 {
                    current_tx.saturating_sub(prev_net_tx) as f64 / dt
                } else {
                    0.0
                };
                prev_net_rx = current_rx;
                prev_net_tx = current_tx;

                (Some(rx_sec), Some(tx_sec), Some(current_rx), Some(current_tx))
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
                // Extended metrics
                cpu_per_core,
                disk_read_bytes_sec: None, // sysinfo doesn't provide disk I/O rates directly
                disk_write_bytes_sec: None,
                disk_used_bytes: disk_used,
                disk_total_bytes: disk_total,
                network_rx_bytes_sec: net_rx_sec,
                network_tx_bytes_sec: net_tx_sec,
                network_rx_total_bytes: net_rx_total,
                network_tx_total_bytes: net_tx_total,
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

/// Dispatch job based on JobSpec to the appropriate processor.
///
/// Returns (rows_processed, result_json, cached_engine).
fn dispatch_job(
    cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    job_spec: &JobSpec,
    filters: &[String],
    intervals: &[String],
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<serde_json::Value>, Option<(String, QueryEngine)>)> {
    // Parse filters from strings
    let key_ranges = parse_filter_strings(filters);
    let interval_list = parse_interval_strings(intervals);

    match job_spec {
        JobSpec::ExportParquet { output_path } => {
            let (rows, engine) = process_parquet_export(
                cached_engine,
                partitions,
                input_path,
                output_path,
                &key_ranges,
                interval_list.as_ref(),
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::ExportJson { output_path, .. } => {
            let (rows, engine) = process_json_export(
                cached_engine,
                partitions,
                input_path,
                output_path,
                &key_ranges,
                interval_list.as_ref(),
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::Summary => {
            let (rows, stats, engine) = process_summary(
                cached_engine,
                partitions,
                input_path,
                telemetry,
            )?;
            // Convert stats to JSON for aggregation on coordinator
            let result_json = serde_json::to_value(&stats).ok();
            Ok((rows, result_json, engine))
        }
        JobSpec::Validate { .. } => {
            // TODO: Implement distributed validation
            eprintln!("Validate job not yet implemented for distributed mode");
            Ok((0, None, cached_engine))
        }
        JobSpec::Manhattan(spec) => {
            let (rows, engine) = process_manhattan_scan(
                cached_engine,
                partitions,
                spec,
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::ManhattanScan(spec) => {
            let (rows, engine) = process_manhattan_scan_v2(
                cached_engine,
                partitions,
                spec,
                telemetry,
            )?;
            Ok((rows, None, engine))
        }
        JobSpec::ManhattanAggregate(spec) => {
            let rows = process_manhattan_aggregate(spec)?;
            Ok((rows, None, None))
        }
        JobSpec::Loci(spec) => {
            let rows = process_loci(spec)?;
            Ok((rows, None, None))
        }
    }
}

/// Process a loci generation job.
fn process_loci(spec: &crate::distributed::message::LociSpec) -> Result<usize> {
    use crate::manhattan::aggregate::generate_loci_standalone;

    println!("Processing loci generation job:");
    println!("  Output dir: {}", spec.output_dir);
    println!("  Exome: {:?}", spec.exome_results);
    println!("  Genome: {:?}", spec.genome_results);
    println!("  Gene burden: {:?}", spec.gene_burden);
    println!("  Window: {}bp", spec.locus_window);
    println!("  Threshold: {}", spec.threshold);
    println!("  Gene threshold: {}", spec.gene_threshold);

    let loci = generate_loci_standalone(
        &spec.output_dir,
        spec.exome_results.as_deref(),
        spec.genome_results.as_deref(),
        spec.gene_burden.as_deref(),
        spec.locus_window,
        spec.threshold,
        spec.gene_threshold,
        8, // threads per worker
    )?;

    println!("Generated {} locus plots", loci.len());
    Ok(loci.len())
}

/// Parse filter strings back into KeyRanges.
///
/// Uses the shared filter parsing module to convert where clause strings
/// into KeyRange objects for row filtering.
fn parse_filter_strings(filters: &[String]) -> Vec<KeyRange> {
    use crate::query::filter::parse_where_condition;

    filters
        .iter()
        .filter_map(|s| {
            let range = parse_where_condition(s);
            if range.is_none() {
                eprintln!("Warning: failed to parse filter condition: {}", s);
            }
            range
        })
        .collect()
}

/// Parse interval strings back into IntervalList.
///
/// Note: Similar to filters, interval parsing is complex and lives in main.rs.
/// For distributed mode, the coordinator handles interval validation.
fn parse_interval_strings(_intervals: &[String]) -> Option<Arc<IntervalList>> {
    // TODO: Move interval parsing to a shared module if needed on workers
    None
}

/// Process partitions and write to Parquet output (synchronous version).
/// Uses rayon to process partitions in parallel across all CPU cores.
fn process_parquet_export(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    output_path: &str,
    filters: &[KeyRange],
    intervals: Option<&Arc<IntervalList>>,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::query::row_matches_intervals;
    use rayon::prelude::*;

    println!("Processing {} partitions to Parquet...", partitions.len());

    let output_is_cloud = is_cloud_path(output_path);

    // Clone refs for the parallel closure
    let input_path = input_path.to_string();
    let output_path = output_path.to_string();
    let filters = filters.to_vec();
    let intervals = intervals.cloned();

    // Process partitions in parallel using rayon
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            // Each thread opens its own QueryEngine (they share underlying caches)
            let engine = QueryEngine::open_path(&input_path)?;
            let row_type = engine.row_type().clone();
            let arrow_schema = Arc::new(crate::parquet::schema::create_schema(&row_type)?);

            // Determine the output file path
            let output_file = if output_is_cloud {
                let base = output_path.trim_end_matches('/');
                format!("{}/part-{:05}.parquet", base, partition_id)
            } else {
                format!("{}/part-{:05}.parquet", output_path, partition_id)
            };

            const BATCH_SIZE: usize = 4096;
            let mut batch_rows = Vec::with_capacity(BATCH_SIZE);
            let mut partition_rows = 0;

            // Stream rows from this partition with filters
            let iter = engine.scan_partition_iter(partition_id, &filters)?;

            // Clone telemetry for this thread
            let ts = telemetry.clone();

            // Helper macro for processing with any writer type
            macro_rules! process_with_writer {
                ($writer:expr) => {{
                    let mut writer = $writer;

                    for row_result in iter {
                        let row = row_result?;

                        // Apply interval filtering if present
                        if let Some(ref ivl) = intervals {
                            if !row_matches_intervals(&row, ivl) {
                                continue;
                            }
                        }

                        batch_rows.push(row);

                        if batch_rows.len() >= BATCH_SIZE {
                            let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                            writer.write_batch(&batch)?;
                            partition_rows += batch_rows.len();
                            // Update telemetry row count
                            if let Some(ref t) = ts {
                                t.total_rows.fetch_add(batch_rows.len(), Ordering::Relaxed);
                            }
                            batch_rows.clear();
                        }
                    }

                    // Write remaining rows
                    if !batch_rows.is_empty() {
                        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
                        writer.write_batch(&batch)?;
                        partition_rows += batch_rows.len();
                        if let Some(ref t) = ts {
                            t.total_rows.fetch_add(batch_rows.len(), Ordering::Relaxed);
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

            println!(
                "  Partition {} complete: {} rows -> {}",
                partition_id, partition_rows, output_file
            );

            Ok(partition_rows)
        })
        .collect();

    // Check for any errors
    for result in &results {
        if let Err(e) = result {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Partition processing failed: {}", e),
            )));
        }
    }

    let total: usize = results.iter().filter_map(|r| r.as_ref().ok()).sum();

    // Don't return cached engine since we opened multiple in parallel
    Ok((total, None))
}

/// Process partitions and write to JSON output (NDJSON format).
fn process_json_export(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    output_path: &str,
    filters: &[KeyRange],
    intervals: Option<&Arc<IntervalList>>,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::export::JsonWriter;
    use crate::query::row_matches_intervals;
    use rayon::prelude::*;
    use std::fs::File;
    use std::io::BufWriter;

    println!("Processing {} partitions to JSON...", partitions.len());

    let output_is_cloud = is_cloud_path(output_path);

    // Clone refs for the parallel closure
    let input_path = input_path.to_string();
    let output_path = output_path.to_string();
    let filters = filters.to_vec();
    let intervals = intervals.cloned();

    // Process partitions in parallel using rayon
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            let engine = QueryEngine::open_path(&input_path)?;

            let output_file = if output_is_cloud {
                let base = output_path.trim_end_matches('/');
                format!("{}/part-{:05}.json", base, partition_id)
            } else {
                format!("{}/part-{:05}.json", output_path, partition_id)
            };

            let mut partition_rows = 0;

            // Stream rows from this partition with filters
            let iter = engine.scan_partition_iter(partition_id, &filters)?;

            let ts = telemetry.clone();

            macro_rules! process_json_with_writer {
                ($writer:expr) => {{
                    let mut json_writer = JsonWriter::new($writer);

                    for row_result in iter {
                        let row = row_result?;

                        // Apply interval filtering if present
                        if let Some(ref ivl) = intervals {
                            if !row_matches_intervals(&row, ivl) {
                                continue;
                            }
                        }

                        json_writer.write_row(&row)?;
                        partition_rows += 1;

                        if let Some(ref t) = ts {
                            t.total_rows.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    json_writer.flush()?;
                    json_writer.into_inner()
                }};
            }

            if output_is_cloud {
                let cloud_writer = StreamingCloudWriter::new(&output_file)?;
                let writer = process_json_with_writer!(cloud_writer);
                writer.finish()?;
            } else {
                let file = File::create(&output_file)?;
                let buf_writer = BufWriter::new(file);
                process_json_with_writer!(buf_writer);
            }

            println!(
                "  Partition {} complete: {} rows -> {}",
                partition_id, partition_rows, output_file
            );

            Ok(partition_rows)
        })
        .collect();

    // Check for any errors
    for result in &results {
        if let Err(e) = result {
            return Err(crate::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Partition processing failed: {}", e),
            )));
        }
    }

    let total: usize = results.iter().filter_map(|r| r.as_ref().ok()).sum();
    Ok((total, None))
}

/// Process partitions and collect summary statistics.
/// Uses rayon to process partitions in parallel and merge stats.
fn process_summary(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    input_path: &str,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, crate::summary::stats::StatsAccumulator, Option<(String, QueryEngine)>)> {
    use crate::summary::stats::StatsAccumulator;
    use rayon::prelude::*;

    println!("Processing {} partitions for summary...", partitions.len());

    let input_path = input_path.to_string();

    // Process partitions in parallel using rayon's fold/reduce
    // Each thread gets its own StatsAccumulator and they are merged at the end
    let (total_rows, stats) = partitions
        .par_iter()
        .fold(
            || (0usize, StatsAccumulator::new()),
            |(mut rows, mut acc), &partition_id| {
                match QueryEngine::open_path(&input_path) {
                    Ok(engine) => {
                        match engine.scan_partition_iter(partition_id, &[]) {
                            Ok(iter) => {
                                for row_result in iter {
                                    match row_result {
                                        Ok(row) => {
                                            acc.process_row(&row);
                                            rows += 1;

                                            // Update telemetry
                                            if let Some(ref t) = telemetry {
                                                t.total_rows.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Warning: Error reading row in partition {}: {}",
                                                partition_id, e
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "Warning: Failed to scan partition {}: {}",
                                    partition_id, e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to open engine for partition {}: {}", partition_id, e);
                    }
                }
                (rows, acc)
            },
        )
        .reduce(
            || (0, StatsAccumulator::new()),
            |(rows_a, mut acc_a), (rows_b, acc_b)| {
                acc_a.merge(acc_b);
                (rows_a + rows_b, acc_a)
            },
        );

    println!("Summary complete: {} rows processed, {} fields tracked", total_rows, stats.stats.len());

    // Don't cache engine since we opened multiple in parallel
    Ok((total_rows, stats, None))
}

/// Process partitions for a Manhattan plot job.
///
/// Renders plot points directly to a PNG image for this batch of partitions.
/// The coordinator will composite all partial PNGs into the final image.
fn process_manhattan_scan(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    spec: &ManhattanSpec,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::io::{is_cloud_path, StreamingCloudWriter};
    use crate::manhattan::data::{extract_plot_data, PlotPoint};
    use crate::manhattan::render::ManhattanRenderer;
    use rayon::prelude::*;
    use std::io::Write;

    println!(
        "Processing {} partitions for Manhattan plot...",
        partitions.len()
    );

    // Get the pre-computed layout from the spec
    let layout = spec.layout.as_ref().ok_or_else(|| {
        crate::HailError::InvalidFormat("Manhattan job missing pre-computed layout".into())
    })?;
    let y_scale = spec.y_scale.as_ref().ok_or_else(|| {
        crate::HailError::InvalidFormat("Manhattan job missing pre-computed y_scale".into())
    })?;

    // Determine which table to scan
    let table_path = spec
        .genome
        .as_ref()
        .or(spec.exome.as_ref())
        .ok_or_else(|| {
            crate::HailError::InvalidFormat("Manhattan job requires --genome or --exome".into())
        })?;

    let part_id = partitions.first().copied().unwrap_or(0);

    // Update telemetry with active partition
    if let Some(ref ts) = telemetry {
        ts.active_partition.store(part_id, Ordering::Relaxed);
    }

    let y_field = spec.y_field.clone();
    let table_path_clone = table_path.clone();
    let output_base = spec.output_path.trim_end_matches('/').to_string();
    let width = spec.width;
    let height = spec.height;

    // Parallel scan+render+encode: each partition produces its own PNG
    // This parallelizes PNG encoding across all cores
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            let engine = QueryEngine::open_path(&table_path_clone)?;
            let iter = engine.scan_partition_iter(partition_id, &[])?;

            // Each thread has its own renderer with transparent background
            let mut renderer = ManhattanRenderer::new_transparent(width, height);
            let mut rows = 0usize;

            for row_result in iter {
                let row = row_result?;
                rows += 1;

                if let Some(point) = extract_plot_data(&row, &y_field) {
                    // Normalize contig name (strip "chr" prefix)
                    let contig_name = if point.contig.starts_with("chr") {
                        &point.contig[3..]
                    } else {
                        &point.contig
                    };

                    // Map to pixel coordinates and render
                    if let Some(x) = layout.get_x(contig_name, point.position) {
                        let y = y_scale.get_y(point.neg_log10_p);
                        let color = layout.get_color(contig_name);
                        renderer.render_point(x, y, color, 0.6);
                    }
                }
            }

            // Encode PNG (now parallel - each thread encodes its own)
            let png_data = renderer.encode_png()?;

            // Write PNG for this partition
            let output_file = format!("{}/part-{:05}.png", output_base, partition_id);
            if is_cloud_path(&output_file) {
                let mut writer = StreamingCloudWriter::new(&output_file)?;
                writer.write_all(&png_data)?;
                writer.finish()?;
            } else {
                std::fs::write(&output_file, &png_data)?;
            }

            Ok(rows)
        })
        .collect();

    // Aggregate row counts
    let mut total_rows = 0usize;
    for result in results {
        total_rows += result?;
    }

    // Update telemetry
    if let Some(ref ts) = telemetry {
        ts.total_rows.fetch_add(total_rows, Ordering::Relaxed);
    }

    println!(
        "  Manhattan partitions {:?} complete: {} rows -> {}/part-*.png",
        partitions, total_rows, output_base
    );

    // Don't cache engine since we opened multiple in parallel
    Ok((total_rows, None))
}

/// Process partitions for Manhattan scan phase (V2 pipeline).
///
/// This is the Phase 1 worker task. For each partition, it:
/// 1. Renders plot points to a partial PNG (transparent background)
/// 2. Writes significant hits (below threshold) to a Parquet file
///
/// Output structure:
/// - {output_path}/{source}/part-{id}.png
/// - {output_path}/{source}/part-{id}-sig.parquet
fn process_manhattan_scan_v2(
    _cached_engine: Option<(String, QueryEngine)>,
    partitions: &[usize],
    spec: &ManhattanScanSpec,
    telemetry: Option<Arc<TelemetryState>>,
) -> Result<(usize, Option<(String, QueryEngine)>)> {
    use crate::io::{is_cloud_path, StreamingCloudWriter};
    use crate::manhattan::data::{extract_plot_data, SigHitRow};
    use crate::manhattan::render::ManhattanRenderer;
    use crate::manhattan::sig_writer::SigHitWriter;
    use rayon::prelude::*;
    use std::io::Write;

    let source_name = match spec.source {
        ManhattanSource::Exome => "exome",
        ManhattanSource::Genome => "genome",
    };

    println!(
        "Processing {} partitions for Manhattan scan ({})...",
        partitions.len(),
        source_name
    );

    let layout = &spec.layout;
    let y_scale = &spec.y_scale;
    let table_path = &spec.table_path;
    let output_base = format!("{}/{}", spec.output_path.trim_end_matches('/'), source_name);
    let width = spec.width;
    let height = spec.height;
    let threshold = spec.threshold;
    let y_field = &spec.y_field;

    // Update telemetry with first partition
    if let Some(ref ts) = telemetry {
        if let Some(&part_id) = partitions.first() {
            ts.active_partition.store(part_id, Ordering::Relaxed);
        }
    }

    // Parallel scan+render+encode: each partition produces PNG + sig.parquet
    let results: Vec<Result<usize>> = partitions
        .par_iter()
        .map(|&partition_id| {
            let engine = QueryEngine::open_path(table_path)?;
            let iter = engine.scan_partition_iter(partition_id, &[])?;

            // Each thread has its own renderer with transparent background
            let mut renderer = ManhattanRenderer::new_transparent(width, height);
            let mut rows = 0usize;

            // Collect significant hits for this partition
            let mut sig_hits: Vec<SigHitRow> = Vec::new();

            for row_result in iter {
                let row = row_result?;
                rows += 1;

                if let Some(point) = extract_plot_data(&row, y_field) {
                    // Normalize contig name (strip "chr" prefix)
                    let contig_name = if point.contig.starts_with("chr") {
                        &point.contig[3..]
                    } else {
                        &point.contig
                    };

                    // Map to pixel coordinates and render
                    if let Some(x) = layout.get_x(contig_name, point.position) {
                        let y = y_scale.get_y(point.neg_log10_p);
                        let color = layout.get_color(contig_name);
                        renderer.render_point(x, y, color, 0.6);
                    }

                    // Check significance threshold
                    if point.pvalue < threshold {
                        // Extract additional fields for significant hit
                        let (ref_allele, alt_allele, beta, se, af) =
                            extract_sig_hit_fields(&row);

                        sig_hits.push(SigHitRow {
                            contig: contig_name.to_string(),
                            position: point.position,
                            ref_allele,
                            alt_allele,
                            pvalue: point.pvalue,
                            beta,
                            se,
                            af,
                        });
                    }
                }
            }

            // Encode PNG
            let png_data = renderer.encode_png()?;

            // Write PNG for this partition
            let png_file = format!("{}/part-{:05}.png", output_base, partition_id);
            if is_cloud_path(&png_file) {
                let mut writer = StreamingCloudWriter::new(&png_file)?;
                writer.write_all(&png_data)?;
                writer.finish()?;
            } else {
                std::fs::create_dir_all(&output_base)?;
                std::fs::write(&png_file, &png_data)?;
            }

            // Write significant hits Parquet (even if empty - consistent output)
            let sig_file = format!("{}/part-{:05}-sig.parquet", output_base, partition_id);
            if is_cloud_path(&sig_file) {
                let cloud_writer = crate::io::CloudWriter::new(&sig_file)?;
                let mut writer = SigHitWriter::from_writer(cloud_writer)?;
                for hit in sig_hits {
                    writer.write(hit)?;
                }
                // Get back the cloud writer and finish to trigger the upload
                let cloud_writer = writer.into_inner()?;
                cloud_writer.finish()?;
            } else {
                let mut writer = SigHitWriter::new(&sig_file)?;
                for hit in sig_hits {
                    writer.write(hit)?;
                }
                writer.finish()?;
            }

            Ok(rows)
        })
        .collect();

    // Aggregate row counts
    let mut total_rows = 0usize;
    for result in results {
        total_rows += result?;
    }

    // Update telemetry
    if let Some(ref ts) = telemetry {
        ts.total_rows.fetch_add(total_rows, Ordering::Relaxed);
    }

    println!(
        "  Manhattan scan ({}) partitions {:?} complete: {} rows",
        source_name, partitions, total_rows
    );

    Ok((total_rows, None))
}


/// Extract fields for a significant hit from an encoded row.
fn extract_sig_hit_fields(row: &crate::codec::EncodedValue) -> (String, String, Option<f64>, Option<f64>, Option<f64>) {
    use crate::codec::EncodedValue;

    // Helper to get nested field
    fn get_field<'a>(value: &'a EncodedValue, path: &[&str]) -> Option<&'a EncodedValue> {
        let mut current = value;
        for &field_name in path {
            if let EncodedValue::Struct(fields) = current {
                current = fields.iter().find(|(n, _)| n == field_name).map(|(_, v)| v)?;
            } else {
                return None;
            }
        }
        Some(current)
    }

    // Extract alleles
    let (ref_allele, alt_allele) = if let Some(EncodedValue::Array(alleles)) = get_field(row, &["alleles"]) {
        let ref_a = alleles.first()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        let alt_a = alleles.get(1)
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        (ref_a, alt_a)
    } else {
        (String::new(), String::new())
    };

    // Extract BETA
    let beta = get_field(row, &["BETA"])
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        });

    // Extract SE
    let se = get_field(row, &["SE"])
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        });

    // Extract AF (AF_Allele2)
    let af = get_field(row, &["AF_Allele2"])
        .and_then(|v| match v {
            EncodedValue::Float64(f) => Some(*f),
            EncodedValue::Float32(f) => Some(*f as f64),
            _ => None,
        });

    (ref_allele, alt_allele, beta, se, af)
}

/// Process Manhattan aggregate phase (V2 pipeline).
///
/// This is the Phase 2 worker task. It:
/// 1. Composites partial PNGs into final Manhattan plots
/// 2. Processes gene burden table (if provided)
/// 3. Merges pre-annotated significant hits
/// 4. Generates locus plots for significant regions
/// 5. Writes manifest.json
fn process_manhattan_aggregate(
    spec: &ManhattanAggregateSpec,
) -> Result<usize> {
    use crate::manhattan::aggregate::run_aggregation;
    run_aggregation(spec)
}

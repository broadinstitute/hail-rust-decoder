//! Coordinator server for distributed processing.
//!
//! The coordinator maintains the state of a distributed job:
//! - Queue of pending partitions
//! - Map of partitions currently being processed (with timestamps)
//! - Set of completed partitions
//!
//! Workers poll `/work` to get assignments and `/complete` to report completion.
//! A background task monitors for timed-out workers and reschedules their work.

use crate::distributed::message::{
    CancelRequest, CancelResponse, CompleteRequest, CompleteResponse, DashboardMetrics,
    DashboardSummary, DashboardWorker, ExportMetricsRequest, ExportMetricsResponse,
    HeartbeatRequest, HeartbeatResponse, JobConfigRequest, JobConfigResponse, JobResultResponse,
    JobSpec, ManhattanAggregateSpec, ManhattanScanSpec, ManhattanSource, ManhattanSpec,
    StatusResponse, TelemetrySnapshot, WorkRequest, WorkResponse, WorkerMetricsSeries,
};
use crate::distributed::metrics_db::MetricsDb;
use crate::Result;
use axum::body::Body;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Configuration for the coordinator.
pub struct CoordinatorConfig {
    /// Port to listen on
    pub port: u16,
    /// Path to input Hail table
    pub input_path: String,
    /// Job specification (what operation to perform)
    pub job_spec: Option<JobSpec>,
    /// Total number of partitions to process
    pub total_partitions: usize,
    /// Number of partitions to assign per work request (batching)
    pub batch_size: usize,
    /// Timeout before rescheduling work (seconds)
    pub timeout_secs: u64,
    /// Timeout for stuck jobs making no progress (seconds)
    pub stuck_timeout_secs: u64,
    /// Filter conditions (where clauses)
    pub filters: Vec<String>,
    /// Interval filters
    pub intervals: Vec<String>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            input_path: String::new(),
            job_spec: None,
            total_partitions: 0,
            batch_size: 1, // Default to 1 partition per request for fine-grained retry
            timeout_secs: 600, // 10 minutes
            stuck_timeout_secs: 600, // 10 minutes for stuck job detection
            filters: Vec::new(),
            intervals: Vec::new(),
        }
    }
}

/// Maximum number of telemetry snapshots to keep per worker.
const MAX_METRICS_HISTORY: usize = 300; // ~10 min at 2s intervals

/// Seconds without a heartbeat before a worker is considered suspect.
const WORKER_SUSPECT_TIMEOUT_SECS: u64 = 30;

/// Current status of a worker.
#[derive(Debug, Clone, PartialEq)]
enum WorkerStatus {
    /// Worker is actively sending heartbeats
    Active,
    /// Worker is idle (requested work, got Wait)
    Idle,
    /// Worker has not sent a heartbeat recently
    SuspectedDead,
}

impl WorkerStatus {
    fn as_str(&self) -> &'static str {
        match self {
            WorkerStatus::Active => "active",
            WorkerStatus::Idle => "idle",
            WorkerStatus::SuspectedDead => "suspected_dead",
        }
    }
}

/// Phase of a Manhattan pipeline job.
#[derive(Debug, Clone, PartialEq)]
enum ManhattanPhase {
    /// Scanning partitions (outputs partial PNGs + sig.parquet)
    Scan,
    /// Aggregating results (compositing, annotation joins, locus plots)
    Aggregate,
    /// Pipeline complete
    Complete,
}

/// Tracks what a specific task_id corresponds to in batch mode.
#[derive(Debug, Clone)]
enum ActiveTask {
    /// A scan task for a specific phenotype and partition
    Scan {
        /// Unique ID for the phenotype (e.g., analysis_id)
        phenotype_id: String,
        /// Which partition is being scanned
        partition_id: usize,
        /// Source (exome or genome)
        source: ManhattanSource,
    },
    /// An aggregate batch task processing multiple phenotypes
    AggregateBatch {
        /// IDs of phenotypes being aggregated
        phenotype_ids: Vec<String>,
    },
}

/// State for managing a batch of Manhattan phenotypes.
#[derive(Debug)]
struct BatchState {
    /// Phenotypes waiting to be activated (lazy loading)
    pending_queue: VecDeque<ManhattanSpec>,
    /// Currently active phenotypes: phenotype_id -> pipeline state
    active_phenotypes: HashMap<String, ManhattanPipelineState>,
    /// Phenotypes that have finished scanning and are ready to aggregate
    ready_to_aggregate: Vec<(String, ManhattanAggregateSpec)>,
    /// Count of successfully completed phenotypes
    completed_count: usize,
    /// Count of failed phenotypes
    failed_count: usize,
    /// Total number of phenotypes in the batch
    total_phenotypes: usize,
}

/// Maximum number of phenotypes to process concurrently.
const BATCH_ACTIVE_LIMIT: usize = 20;

/// Minimum batch size for aggregation (unless no other work available).
const AGGREGATE_BATCH_SIZE: usize = 10;

/// State for tracking Manhattan pipeline phases.
#[derive(Debug)]
struct ManhattanPipelineState {
    /// Current phase
    phase: ManhattanPhase,
    /// Original ManhattanSpec from job submission
    original_spec: ManhattanSpec,
    /// Pre-computed layout (shared by scan and aggregate)
    layout: Option<crate::manhattan::layout::ChromosomeLayout>,
    /// Pre-computed Y scale
    y_scale: Option<crate::manhattan::layout::YScale>,

    // Exome scan tracking
    /// Total exome partitions
    exome_total_partitions: usize,
    /// Pending exome partitions
    exome_pending: VecDeque<usize>,
    /// Processing exome partitions: partition_id -> (worker_id, start_time)
    exome_processing: HashMap<usize, (String, Instant)>,
    /// Completed exome partitions
    exome_completed: HashSet<usize>,

    // Genome scan tracking
    /// Total genome partitions
    genome_total_partitions: usize,
    /// Pending genome partitions
    genome_pending: VecDeque<usize>,
    /// Processing genome partitions: partition_id -> (worker_id, start_time)
    genome_processing: HashMap<usize, (String, Instant)>,
    /// Completed genome partitions
    genome_completed: HashSet<usize>,

    /// Whether aggregate task has been dispatched
    aggregate_dispatched: bool,
    /// Whether aggregate task is complete
    aggregate_complete: bool,
}

/// Tracked state for a single worker.
struct WorkerState {
    /// Last time we heard from this worker (heartbeat or work request)
    last_seen: Instant,
    /// Current status
    status: WorkerStatus,
    /// Recent telemetry snapshots (newest last)
    metrics_history: VecDeque<TelemetrySnapshot>,
    /// Total rows reported completed by this worker
    total_rows: usize,
    /// Total partitions completed by this worker
    partitions_completed: usize,
}

/// Internal state of the coordinator.
struct CoordinatorData {
    /// Partitions waiting to be assigned
    pending_partitions: VecDeque<usize>,
    /// Partitions currently being processed: partition_id -> (worker_id, start_time)
    processing_partitions: HashMap<usize, (String, Instant)>,
    /// Partitions that have been completed
    completed_partitions: HashSet<usize>,
    /// Configuration
    config: CoordinatorConfig,
    /// Total rows processed (reported by workers)
    total_rows: usize,
    /// Track retry counts per partition
    retry_counts: HashMap<usize, usize>,
    /// Partitions that permanently failed (exceeded max retries)
    failed_partitions: HashSet<usize>,
    /// Registry of all known workers and their telemetry
    worker_registry: HashMap<String, WorkerState>,
    /// When the job started
    job_start_time: Instant,
    /// Last time progress was made (job start or last partition completion)
    last_progress_time: Instant,
    /// Whether the coordinator is idle (waiting for job submission via /api/job)
    idle: bool,
    /// SQLite database for persistent metrics storage
    metrics_db: MetricsDb,
    /// Aggregated results from workers (for Summary/Validate jobs)
    aggregated_results: Vec<serde_json::Value>,
    /// Manhattan pipeline state (only set for single Manhattan jobs)
    manhattan_state: Option<ManhattanPipelineState>,
    /// Batch state (only set for ManhattanBatch jobs)
    batch_state: Option<BatchState>,
    /// Active tasks: task_id -> ActiveTask (for batch mode tracking)
    active_tasks: HashMap<String, ActiveTask>,
    /// Last error message from a failed task
    last_error: Option<String>,
}

type SharedState = Arc<Mutex<CoordinatorData>>;

/// Start the coordinator server using a config struct.
///
/// This function blocks until the job is complete or the server is interrupted.
pub async fn start_coordinator(config: CoordinatorConfig) -> Result<()> {
    // Extract output_path from job_spec for backward compatibility
    let output_path = config
        .job_spec
        .as_ref()
        .and_then(|js| js.output_path())
        .map(String::from)
        .unwrap_or_default();

    run_coordinator(
        config.port,
        config.input_path,
        output_path,
        config.total_partitions,
        config.batch_size,
        config.timeout_secs,
    )
    .await
}

/// Properly structured coordinator startup.
///
/// Note: For backward compatibility, `output_path` is converted to a default
/// ExportParquet JobSpec. New code should use the API endpoint with JobSpec directly.
pub async fn run_coordinator(
    port: u16,
    input_path: String,
    output_path: String,
    total_partitions: usize,
    batch_size: usize,
    timeout_secs: u64,
) -> Result<()> {
    use axum::{routing::{get, post}, Router};
    use tokio::net::TcpListener;

    // Determine if starting in idle mode (no job configured yet)
    let idle = total_partitions == 0;

    // Convert output_path to JobSpec for backward compatibility
    let job_spec = if output_path.is_empty() {
        None
    } else {
        Some(JobSpec::ExportParquet {
            output_path: output_path.clone(),
        })
    };

    if idle {
        println!(
            "Coordinator starting on port {} in IDLE mode (waiting for job submission)",
            port
        );
        println!("  Submit a job via POST /api/job or pool submit");
    } else {
        println!(
            "Coordinator starting on port {} with {} partitions",
            port, total_partitions
        );
        println!("  Input: {}", input_path);
        if let Some(ref spec) = job_spec {
            println!("  Job: {}", spec.description());
            if let Some(out) = spec.output_path() {
                println!("  Output: {}", out);
            }
        }
    }
    println!("  Batch size: {}", batch_size);
    println!("  Timeout: {}s", timeout_secs);

    // Initialize SQLite database for metrics persistence
    let metrics_db = MetricsDb::open("/tmp/hail-coordinator-metrics.db").map_err(|e| {
        crate::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to open metrics database: {}", e),
        ))
    })?;
    println!("  Metrics DB: /tmp/hail-coordinator-metrics.db");

    let state = Arc::new(Mutex::new(CoordinatorData {
        pending_partitions: (0..total_partitions).collect(),
        processing_partitions: HashMap::new(),
        completed_partitions: HashSet::new(),
        config: CoordinatorConfig {
            port,
            input_path,
            job_spec,
            total_partitions,
            batch_size,
            timeout_secs,
            stuck_timeout_secs: 600, // Default 10 minutes
            filters: Vec::new(),
            intervals: Vec::new(),
        },
        total_rows: 0,
        retry_counts: HashMap::new(),
        failed_partitions: HashSet::new(),
        worker_registry: HashMap::new(),
        job_start_time: Instant::now(),
        last_progress_time: Instant::now(),
        idle,
        metrics_db,
        aggregated_results: Vec::new(),
        manhattan_state: None,
        batch_state: None,
        active_tasks: HashMap::new(),
        last_error: None,
    }));

    // Start background timeout monitor
    let monitor_state = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            check_timeouts(&monitor_state, timeout_secs);
            check_worker_liveness(&monitor_state);

            // Check for stuck jobs (R3)
            let stuck_timeout = {
                let data = monitor_state.lock().unwrap();
                data.config.stuck_timeout_secs
            };
            check_stuck_job(&monitor_state, stuck_timeout);

            // Print periodic status
            // For batch/Manhattan jobs, use their state; otherwise use standard counters
            let (pending, processing, completed, failed, total, rows, is_idle, is_batch_complete) = {
                let data = monitor_state.lock().unwrap();
                if let Some(ref batch) = data.batch_state {
                    // Batch jobs use phenotype-level tracking
                    let total = batch.total_phenotypes;
                    let completed = batch.completed_count;
                    let pending = batch.pending_queue.len();
                    let processing = batch.active_phenotypes.len() + batch.ready_to_aggregate.len();
                    let is_complete = batch.pending_queue.is_empty()
                        && batch.active_phenotypes.is_empty()
                        && batch.ready_to_aggregate.is_empty()
                        && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;
                    (
                        pending,
                        processing,
                        completed,
                        batch.failed_count,
                        total,
                        data.total_rows,
                        data.idle,
                        is_complete,
                    )
                } else if let Some(ref m) = data.manhattan_state {
                    // Single Manhattan pipeline uses separate tracking
                    let total_parts = m.exome_total_partitions + m.genome_total_partitions + 1; // +1 for aggregate
                    let completed_parts = m.exome_completed.len() + m.genome_completed.len() + if m.aggregate_complete { 1 } else { 0 };
                    let processing_parts = m.exome_processing.len() + m.genome_processing.len() + if m.aggregate_dispatched && !m.aggregate_complete { 1 } else { 0 };
                    let pending_parts = m.exome_pending.len() + m.genome_pending.len();
                    let is_complete = m.phase == ManhattanPhase::Complete;
                    (
                        pending_parts,
                        processing_parts,
                        completed_parts,
                        data.failed_partitions.len(),
                        total_parts,
                        data.total_rows,
                        data.idle,
                        is_complete,
                    )
                } else {
                    (
                        data.pending_partitions.len(),
                        data.processing_partitions.len(),
                        data.completed_partitions.len(),
                        data.failed_partitions.len(),
                        data.config.total_partitions,
                        data.total_rows,
                        data.idle,
                        false, // Standard jobs don't use this flag
                    )
                }
            };

            // Don't print progress or exit if idle
            if is_idle {
                println!("Coordinator idle, waiting for job submission...");
                continue;
            }

            if completed > 0 || failed > 0 {
                println!(
                    "Progress: {}/{} partitions ({:.1}%), {} failed, {} rows processed",
                    completed,
                    total,
                    (completed as f64 / total as f64) * 100.0,
                    failed,
                    rows
                );
            }

            // Check if job is complete
            // For batch/Manhattan: use the phase flag; for standard jobs: check partition counts
            let job_complete = is_batch_complete ||
                (total > 0 && (completed + failed) == total && processing == 0 && pending == 0);

            if job_complete {
                if failed > 0 {
                    println!(
                        "Job finished with {} failed partitions out of {}. Total rows: {}",
                        failed, total, rows
                    );
                } else {
                    println!("All {} partitions completed! Total rows: {}", total, rows);
                }

                // For Manhattan jobs, run the composite step to merge partial PNGs
                let manhattan_spec = {
                    let data = monitor_state.lock().unwrap();
                    if let Some(JobSpec::Manhattan(ref spec)) = data.config.job_spec {
                        Some(spec.clone())
                    } else {
                        None
                    }
                };

                if let Some(spec) = manhattan_spec {
                    if spec.skip_composite {
                        println!("Skipping composite step (--no-composite). Run manually with:");
                        println!("  hail-decoder manhattan --from-shards {}", spec.output_path);
                    } else {
                    println!("Running post-job composite for Manhattan plot...");

                    let output_dir = spec.output_path.trim_end_matches('/');
                    let final_png = format!("{}/manhattan.png", output_dir);

                    // Run composite in a blocking thread to avoid nested runtime issues
                    let output_path = spec.output_path.clone();
                    let final_png_clone = final_png.clone();
                    let width = spec.width;
                    let height = spec.height;
                    let threshold = spec.threshold;

                    let result = tokio::task::spawn_blocking(move || {
                        crate::manhattan::pipeline::composite_partial_pngs(
                            &output_path,
                            &final_png_clone,
                            width,
                            height,
                            threshold,
                        )
                    })
                    .await;

                    match result {
                        Ok(Ok(())) => println!("Composite complete: {}", final_png),
                        Ok(Err(e)) => eprintln!("Warning: Composite failed: {}", e),
                        Err(e) => eprintln!("Warning: Composite task panicked: {}", e),
                    }
                    }
                }

                // Save aggregated results to file before exiting
                {
                    let data = monitor_state.lock().unwrap();
                    let result = JobResultResponse {
                        available: true,
                        result: Some(serde_json::Value::Array(data.aggregated_results.clone())),
                        error: None,
                    };
                    if let Ok(json) = serde_json::to_string_pretty(&result) {
                        if let Err(e) = std::fs::write("/tmp/job_result.json", &json) {
                            eprintln!("Warning: Failed to save results to file: {}", e);
                        } else {
                            println!("Results saved to /tmp/job_result.json");
                        }
                    }
                }

                println!("Coordinator will exit in 10 seconds...");
                tokio::time::sleep(Duration::from_secs(10)).await;
                std::process::exit(0);
            }
        }
    });

    let app = Router::new()
        .route("/work", post(get_work))
        .route("/complete", post(complete_work))
        .route("/status", get(get_status))
        .route("/heartbeat", post(handle_heartbeat))
        .route("/dashboard", get(serve_dashboard))
        .route("/api/dashboard/summary", get(get_dashboard_summary))
        .route("/api/dashboard/workers", get(get_dashboard_workers))
        .route("/api/dashboard/metrics", get(get_dashboard_metrics))
        .route("/api/job", post(submit_job))
        .route("/api/cancel", post(cancel_job))
        .route("/api/result", get(get_job_result))
        .route("/api/binary", get(serve_binary))
        .route("/api/export-metrics", post(export_metrics))
        .with_state(state);

    println!("Dashboard available at http://0.0.0.0:{}/dashboard", port);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Coordinator listening on {}", addr);

    let listener = TcpListener::bind(addr)
        .await
        .map_err(crate::HailError::Io)?;
    axum::serve(listener, app)
        .await
        .map_err(|e| crate::HailError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    Ok(())
}

/// Check if the job is stuck (no progress for N minutes) and auto-cancel if needed.
fn check_stuck_job(state: &SharedState, timeout_secs: u64) {
    let mut data = state.lock().unwrap();

    // Only check if job is running
    if data.idle {
        return;
    }

    // Only consider it stuck if ZERO partitions have completed
    // (if some completed but now it's stalled, that's different - likely worker issues handled by timeouts)
    // Check batch state, Manhattan state, and standard counters
    let has_progress = if let Some(ref batch) = data.batch_state {
        // For batch jobs, check if any phenotypes completed
        batch.completed_count > 0 || !batch.active_phenotypes.values().all(|s| {
            s.exome_completed.is_empty() && s.genome_completed.is_empty()
        })
    } else if let Some(ref m) = data.manhattan_state {
        // For single Manhattan jobs, check if any exome/genome partitions completed
        !m.exome_completed.is_empty() || !m.genome_completed.is_empty() || m.aggregate_complete
    } else {
        // For standard jobs, check standard counter
        !data.completed_partitions.is_empty()
    };

    if has_progress {
        return;
    }

    let elapsed = data.last_progress_time.elapsed();
    if elapsed.as_secs() > timeout_secs {
        println!(
            "Job stuck (0 progress for {}s). Auto-cancelling.",
            elapsed.as_secs()
        );

        // Reset state
        data.pending_partitions.clear();
        data.processing_partitions.clear();
        data.manhattan_state = None;
        data.batch_state = None;
        data.active_tasks.clear();
        data.idle = true;
    }
}

/// Check for timed-out work and reschedule.
fn check_timeouts(state: &SharedState, timeout_secs: u64) {
    let mut data = state.lock().unwrap();
    let now = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    let mut timed_out = Vec::new();
    for (part_id, (worker, start_time)) in &data.processing_partitions {
        if now.duration_since(*start_time) > timeout {
            timed_out.push((*part_id, worker.clone()));
        }
    }

    for (part_id, worker) in timed_out {
        data.processing_partitions.remove(&part_id);

        let retries = data.retry_counts.entry(part_id).or_insert(0);
        *retries += 1;

        if *retries > 3 {
            println!(
                "Partition {} exceeded max retries (worker: {}). Marking as failed.",
                part_id, worker
            );
            data.failed_partitions.insert(part_id);
        } else {
            println!(
                "Partition {} timed out (worker: {}), rescheduling (retry {})",
                part_id, worker, retries
            );
            data.pending_partitions.push_front(part_id);
        }
    }
}

/// Ensure a worker exists in the registry and update last_seen.
fn touch_worker(data: &mut CoordinatorData, worker_id: &str) {
    let worker = data
        .worker_registry
        .entry(worker_id.to_string())
        .or_insert_with(|| WorkerState {
            last_seen: Instant::now(),
            status: WorkerStatus::Idle,
            metrics_history: VecDeque::new(),
            total_rows: 0,
            partitions_completed: 0,
        });
    worker.last_seen = Instant::now();
}

/// Activate phenotypes from the pending queue up to the active limit.
///
/// This implements lazy loading - phenotypes are only initialized when
/// there's capacity to process them, rather than all at once at job submission.
fn activate_next_phenotypes(batch: &mut BatchState) {
    while batch.active_phenotypes.len() < BATCH_ACTIVE_LIMIT && !batch.pending_queue.is_empty() {
        let spec = batch.pending_queue.pop_front().unwrap();

        // Generate a unique ID for this phenotype
        // Use the output_path as the ID since it should be unique per phenotype
        let phenotype_id = spec.output_path.clone();

        // Get partition counts from the spec (set by CLI/pool submit)
        // If not set, fall back to 0 (skip that source)
        let exome_partitions = spec.exome_partitions.unwrap_or(0);
        let genome_partitions = spec.genome_partitions.unwrap_or(0);

        if exome_partitions == 0 && genome_partitions == 0 {
            // No partitions to scan - this phenotype needs partition counts
            // For now, skip it with a warning (CLI should set partition counts)
            println!(
                "Warning: Phenotype {} has no partition counts set, skipping",
                phenotype_id
            );
            batch.failed_count += 1;
            continue;
        }

        println!(
            "Activating phenotype {} ({} exome, {} genome partitions)",
            phenotype_id, exome_partitions, genome_partitions
        );

        // Initialize the pipeline state
        let pipeline_state = ManhattanPipelineState {
            phase: ManhattanPhase::Scan,
            original_spec: spec.clone(),
            layout: spec.layout.clone(),
            y_scale: spec.y_scale.clone(),
            exome_total_partitions: exome_partitions,
            exome_pending: (0..exome_partitions).collect(),
            exome_processing: HashMap::new(),
            exome_completed: HashSet::new(),
            genome_total_partitions: genome_partitions,
            genome_pending: (0..genome_partitions).collect(),
            genome_processing: HashMap::new(),
            genome_completed: HashSet::new(),
            aggregate_dispatched: false,
            aggregate_complete: false,
        };

        batch.active_phenotypes.insert(phenotype_id, pipeline_state);
    }
}

/// Get work for a batch Manhattan job (multi-phenotype scheduling).
fn get_batch_work(
    data: &mut CoordinatorData,
    batch: &mut BatchState,
    worker_id: &str,
) -> axum::Json<WorkResponse> {
    let now = Instant::now();
    let partition_batch_size = data.config.batch_size;

    // Step 1: Activate phenotypes to fill the active pool
    activate_next_phenotypes(batch);

    // Step 2: Priority 1 - Check for aggregation batches
    // If we have enough ready or no other work available
    let has_scan_work = batch.active_phenotypes.values().any(|state| {
        !state.exome_pending.is_empty() || !state.genome_pending.is_empty()
    });

    let should_aggregate = batch.ready_to_aggregate.len() >= AGGREGATE_BATCH_SIZE
        || (!batch.ready_to_aggregate.is_empty() && !has_scan_work && batch.pending_queue.is_empty());

    if should_aggregate {
        // Drain aggregation specs (up to batch size)
        let count = std::cmp::min(batch.ready_to_aggregate.len(), AGGREGATE_BATCH_SIZE);
        let specs_to_aggregate: Vec<_> = batch.ready_to_aggregate.drain(..count).collect();

        let phenotype_ids: Vec<String> = specs_to_aggregate.iter().map(|(id, _)| id.clone()).collect();
        let aggregate_specs: Vec<ManhattanAggregateSpec> = specs_to_aggregate.into_iter().map(|(_, spec)| spec).collect();

        let task_id = Uuid::new_v4().to_string();

        // Track this task
        data.active_tasks.insert(
            task_id.clone(),
            ActiveTask::AggregateBatch {
                phenotype_ids: phenotype_ids.clone(),
            },
        );

        // Update worker status
        if let Some(w) = data.worker_registry.get_mut(worker_id) {
            w.status = WorkerStatus::Active;
        }

        println!(
            "Assigned aggregate batch ({} phenotypes) to worker {} [task={}]",
            phenotype_ids.len(), worker_id, &task_id[..8]
        );

        return axum::Json(WorkResponse::Task {
            task_id,
            partitions: vec![0], // Aggregate batch uses spec list, not partitions
            input_path: String::new(),
            job_spec: JobSpec::ManhattanAggregateBatch { specs: aggregate_specs },
            total_partitions: phenotype_ids.len(),
            filters: Vec::new(),
            intervals: Vec::new(),
        });
    }

    // Step 3: Priority 2 - Find scan work from active phenotypes
    for (phenotype_id, state) in batch.active_phenotypes.iter_mut() {
        // Try exome first, then genome
        let (source, partitions, table_path) = if let Some(part_id) = state.exome_pending.pop_front() {
            let mut parts = vec![part_id];
            while parts.len() < partition_batch_size {
                if let Some(p) = state.exome_pending.pop_front() {
                    parts.push(p);
                } else {
                    break;
                }
            }
            for &p in &parts {
                state.exome_processing.insert(p, (worker_id.to_string(), now));
            }
            (
                ManhattanSource::Exome,
                parts,
                state.original_spec.exome.clone().unwrap_or_default(),
            )
        } else if let Some(part_id) = state.genome_pending.pop_front() {
            let mut parts = vec![part_id];
            while parts.len() < partition_batch_size {
                if let Some(p) = state.genome_pending.pop_front() {
                    parts.push(p);
                } else {
                    break;
                }
            }
            for &p in &parts {
                state.genome_processing.insert(p, (worker_id.to_string(), now));
            }
            (
                ManhattanSource::Genome,
                parts,
                state.original_spec.genome.clone().unwrap_or_default(),
            )
        } else {
            // No pending work for this phenotype, continue to next
            continue;
        };

        let task_id = Uuid::new_v4().to_string();

        // Track this task (using first partition as identifier)
        data.active_tasks.insert(
            task_id.clone(),
            ActiveTask::Scan {
                phenotype_id: phenotype_id.clone(),
                partition_id: partitions[0],
                source,
            },
        );

        // Update worker status
        if let Some(w) = data.worker_registry.get_mut(worker_id) {
            w.status = WorkerStatus::Active;
        }

        let source_name = match source {
            ManhattanSource::Exome => "exome",
            ManhattanSource::Genome => "genome",
        };
        println!(
            "Assigned {} partitions {:?} to worker {} for phenotype {} [task={}]",
            source_name, partitions, worker_id, phenotype_id, &task_id[..8]
        );

        // Build ManhattanScanSpec
        let scan_spec = ManhattanScanSpec {
            source,
            table_path,
            output_path: state.original_spec.output_path.clone(),
            threshold: state.original_spec.threshold,
            y_field: state.original_spec.y_field.clone(),
            layout: state.layout.clone().unwrap_or_default(),
            y_scale: state.y_scale.clone().unwrap_or_default(),
            width: state.original_spec.width,
            height: state.original_spec.height,
        };

        return axum::Json(WorkResponse::Task {
            task_id,
            partitions,
            input_path: String::new(),
            job_spec: JobSpec::ManhattanScan(scan_spec),
            total_partitions: state.exome_total_partitions + state.genome_total_partitions,
            filters: Vec::new(),
            intervals: Vec::new(),
        });
    }

    // Step 4: Check if batch is complete
    let all_done = batch.pending_queue.is_empty()
        && batch.active_phenotypes.is_empty()
        && batch.ready_to_aggregate.is_empty();

    if all_done {
        println!(
            "Manhattan batch complete: {} completed, {} failed",
            batch.completed_count, batch.failed_count
        );
        return axum::Json(WorkResponse::Exit);
    }

    // Step 5: Work in progress, tell worker to wait
    if let Some(w) = data.worker_registry.get_mut(worker_id) {
        w.status = WorkerStatus::Idle;
    }
    axum::Json(WorkResponse::Wait)
}

/// Handler for POST /work - worker requests work.
async fn get_work(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<WorkRequest>,
) -> axum::Json<WorkResponse> {
    let mut data = state.lock().unwrap();
    touch_worker(&mut data, &req.worker_id);

    // If coordinator is idle (no job configured), tell workers to wait
    if data.idle {
        if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
            w.status = WorkerStatus::Idle;
        }
        return axum::Json(WorkResponse::Wait);
    }

    // Check for Manhattan batch job (multi-phenotype scheduling)
    if data.batch_state.is_some() {
        let mut batch = data.batch_state.take().unwrap();
        let result = get_batch_work(&mut data, &mut batch, &req.worker_id);
        data.batch_state = Some(batch);
        return result;
    }

    // Check for Manhattan pipeline job (two-phase execution)
    if data.manhattan_state.is_some() {
        // Take manhattan state out temporarily to avoid borrow issues
        let mut manhattan = data.manhattan_state.take().unwrap();
        let result = get_manhattan_work(&mut data, &mut manhattan, &req.worker_id);
        data.manhattan_state = Some(manhattan);
        return result;
    }

    // Standard (non-Manhattan) job: check if there's pending work
    if let Some(part_id) = data.pending_partitions.pop_front() {
        // Collect batch of partitions
        let mut partitions = vec![part_id];
        let batch_size = data.config.batch_size;

        while partitions.len() < batch_size {
            if let Some(next_id) = data.pending_partitions.pop_front() {
                partitions.push(next_id);
            } else {
                break;
            }
        }

        // Mark all as processing
        let now = Instant::now();
        for &p in &partitions {
            data.processing_partitions
                .insert(p, (req.worker_id.clone(), now));
        }

        // Update worker status
        if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
            w.status = WorkerStatus::Active;
        }

        println!(
            "Assigned partitions {:?} to worker {} ({} pending, {} processing, {} complete)",
            partitions,
            req.worker_id,
            data.pending_partitions.len(),
            data.processing_partitions.len(),
            data.completed_partitions.len()
        );

        // Get job_spec, or return Wait if not configured (shouldn't happen since we check idle)
        let job_spec = match data.config.job_spec.clone() {
            Some(spec) => spec,
            None => {
                // Put partitions back
                for p in partitions.into_iter().rev() {
                    data.pending_partitions.push_front(p);
                }
                return axum::Json(WorkResponse::Wait);
            }
        };

        // Generate unique task ID for tracking
        let task_id = Uuid::new_v4().to_string();

        axum::Json(WorkResponse::Task {
            task_id,
            partitions,
            input_path: data.config.input_path.clone(),
            job_spec,
            total_partitions: data.config.total_partitions,
            filters: data.config.filters.clone(),
            intervals: data.config.intervals.clone(),
        })
    } else if !data.processing_partitions.is_empty() {
        // Work in progress but nothing pending - tell worker to wait
        if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
            w.status = WorkerStatus::Idle;
        }
        axum::Json(WorkResponse::Wait)
    } else {
        // All done
        println!(
            "Worker {} requested work, but all partitions complete",
            req.worker_id
        );
        axum::Json(WorkResponse::Exit)
    }
}

/// Get work for a Manhattan pipeline job (two-phase execution).
fn get_manhattan_work(
    data: &mut CoordinatorData,
    manhattan: &mut ManhattanPipelineState,
    worker_id: &str,
) -> axum::Json<WorkResponse> {
    let now = Instant::now();
    let batch_size = data.config.batch_size;

    // Generate unique task ID for tracking
    let task_id = Uuid::new_v4().to_string();

    match manhattan.phase {
        ManhattanPhase::Scan => {
            // Try to get exome work first, then genome
            let (source, partitions, table_path) =
                if let Some(part_id) = manhattan.exome_pending.pop_front() {
                    let mut parts = vec![part_id];
                    while parts.len() < batch_size {
                        if let Some(p) = manhattan.exome_pending.pop_front() {
                            parts.push(p);
                        } else {
                            break;
                        }
                    }
                    for &p in &parts {
                        manhattan.exome_processing.insert(p, (worker_id.to_string(), now));
                    }
                    (
                        ManhattanSource::Exome,
                        parts,
                        manhattan.original_spec.exome.clone().unwrap_or_default(),
                    )
                } else if let Some(part_id) = manhattan.genome_pending.pop_front() {
                    let mut parts = vec![part_id];
                    while parts.len() < batch_size {
                        if let Some(p) = manhattan.genome_pending.pop_front() {
                            parts.push(p);
                        } else {
                            break;
                        }
                    }
                    for &p in &parts {
                        manhattan.genome_processing.insert(p, (worker_id.to_string(), now));
                    }
                    (
                        ManhattanSource::Genome,
                        parts,
                        manhattan.original_spec.genome.clone().unwrap_or_default(),
                    )
                } else if !manhattan.exome_processing.is_empty()
                    || !manhattan.genome_processing.is_empty()
                {
                    // Scan work still processing, tell worker to wait
                    if let Some(w) = data.worker_registry.get_mut(worker_id) {
                        w.status = WorkerStatus::Idle;
                    }
                    return axum::Json(WorkResponse::Wait);
                } else {
                    // All scan work complete, transition to Aggregate phase
                    println!("Manhattan scan phase complete, transitioning to Aggregate phase");
                    manhattan.phase = ManhattanPhase::Aggregate;
                    return get_manhattan_work(data, manhattan, worker_id);
                };

            // Update worker status
            if let Some(w) = data.worker_registry.get_mut(worker_id) {
                w.status = WorkerStatus::Active;
            }

            let source_name = match source {
                ManhattanSource::Exome => "exome",
                ManhattanSource::Genome => "genome",
            };
            println!(
                "Assigned {} partitions {:?} to worker {} (scan phase)",
                source_name, partitions, worker_id
            );

            // Build ManhattanScanSpec
            let scan_spec = ManhattanScanSpec {
                source,
                table_path,
                output_path: manhattan.original_spec.output_path.clone(),
                threshold: manhattan.original_spec.threshold,
                y_field: manhattan.original_spec.y_field.clone(),
                layout: manhattan.layout.clone().unwrap_or_default(),
                y_scale: manhattan.y_scale.clone().unwrap_or_default(),
                width: manhattan.original_spec.width,
                height: manhattan.original_spec.height,
            };

            axum::Json(WorkResponse::Task {
                task_id,
                partitions,
                input_path: String::new(), // Not used for ManhattanScan
                job_spec: JobSpec::ManhattanScan(scan_spec),
                total_partitions: manhattan.exome_total_partitions + manhattan.genome_total_partitions,
                filters: Vec::new(),
                intervals: Vec::new(),
            })
        }

        ManhattanPhase::Aggregate => {
            if manhattan.aggregate_dispatched && !manhattan.aggregate_complete {
                // Aggregate in progress, tell worker to wait
                if let Some(w) = data.worker_registry.get_mut(worker_id) {
                    w.status = WorkerStatus::Idle;
                }
                return axum::Json(WorkResponse::Wait);
            }

            if manhattan.aggregate_complete {
                // All done
                manhattan.phase = ManhattanPhase::Complete;
                return axum::Json(WorkResponse::Exit);
            }

            // Dispatch aggregate task
            manhattan.aggregate_dispatched = true;

            if let Some(w) = data.worker_registry.get_mut(worker_id) {
                w.status = WorkerStatus::Active;
            }

            println!(
                "Assigned aggregate task to worker {} (aggregate phase)",
                worker_id
            );

            // Build ManhattanAggregateSpec
            let aggregate_spec = ManhattanAggregateSpec {
                output_path: manhattan.original_spec.output_path.clone(),
                exome_results: manhattan.original_spec.exome.clone(),
                genome_results: manhattan.original_spec.genome.clone(),
                gene_burden: manhattan.original_spec.gene_burden.clone(),
                exome_annotations: manhattan.original_spec.exome_annotations.clone(),
                genome_annotations: manhattan.original_spec.genome_annotations.clone(),
                genes: manhattan.original_spec.genes.clone(),
                threshold: manhattan.original_spec.threshold,
                gene_threshold: manhattan.original_spec.gene_threshold,
                locus_threshold: manhattan.original_spec.locus_threshold,
                locus_window: manhattan.original_spec.locus_window,
                locus_plots: manhattan.original_spec.locus_plots,
                width: manhattan.original_spec.width,
                height: manhattan.original_spec.height,
                layout: manhattan.layout.clone().unwrap_or_default(),
                y_scale: manhattan.y_scale.clone().unwrap_or_default(),
                cleanup: false, // TODO: Add cleanup option to ManhattanSpec
            };

            axum::Json(WorkResponse::Task {
                task_id,
                partitions: vec![0], // Single aggregate task
                input_path: String::new(),
                job_spec: JobSpec::ManhattanAggregate(aggregate_spec),
                total_partitions: 1,
                filters: Vec::new(),
                intervals: Vec::new(),
            })
        }

        ManhattanPhase::Complete => {
            axum::Json(WorkResponse::Exit)
        }
    }
}

/// Handle completion for Manhattan pipeline jobs.
fn complete_manhattan_work(
    manhattan: &mut ManhattanPipelineState,
    req: &CompleteRequest,
    last_progress_time: &mut Instant,
) {
    *last_progress_time = Instant::now();

    match manhattan.phase {
        ManhattanPhase::Scan => {
            // Try to find partitions in exome or genome processing maps
            for &part_id in &req.partitions {
                if manhattan.exome_processing.remove(&part_id).is_some() {
                    manhattan.exome_completed.insert(part_id);
                    println!(
                        "Exome partition {} complete ({}/{} exome done)",
                        part_id,
                        manhattan.exome_completed.len(),
                        manhattan.exome_total_partitions
                    );
                } else if manhattan.genome_processing.remove(&part_id).is_some() {
                    manhattan.genome_completed.insert(part_id);
                    println!(
                        "Genome partition {} complete ({}/{} genome done)",
                        part_id,
                        manhattan.genome_completed.len(),
                        manhattan.genome_total_partitions
                    );
                } else {
                    // Partition wasn't in processing (maybe timed out and reassigned)
                    println!(
                        "Warning: partition {} completed but wasn't in processing map",
                        part_id
                    );
                }
            }

            // Check if scan phase is complete
            let exome_done = manhattan.exome_completed.len() == manhattan.exome_total_partitions;
            let genome_done = manhattan.genome_completed.len() == manhattan.genome_total_partitions;
            let exome_idle = manhattan.exome_pending.is_empty() && manhattan.exome_processing.is_empty();
            let genome_idle = manhattan.genome_pending.is_empty() && manhattan.genome_processing.is_empty();

            if (exome_done || manhattan.exome_total_partitions == 0)
                && (genome_done || manhattan.genome_total_partitions == 0)
                && exome_idle
                && genome_idle
            {
                println!(
                    "Manhattan scan phase complete: {} exome, {} genome partitions done",
                    manhattan.exome_completed.len(),
                    manhattan.genome_completed.len()
                );
                manhattan.phase = ManhattanPhase::Aggregate;
            }
        }

        ManhattanPhase::Aggregate => {
            // Aggregate task completed
            manhattan.aggregate_complete = true;
            manhattan.phase = ManhattanPhase::Complete;
            println!("Manhattan aggregate phase complete - job finished!");
        }

        ManhattanPhase::Complete => {
            // Already complete, nothing to do
        }
    }
}

/// Handle completion for batch Manhattan jobs.
///
/// Uses task_id to lookup the active task and update the appropriate state.
fn complete_batch_work(
    data: &mut CoordinatorData,
    batch: &mut BatchState,
    req: &CompleteRequest,
) {
    data.last_progress_time = Instant::now();

    // Look up the task by task_id
    let task = match data.active_tasks.remove(&req.task_id) {
        Some(task) => task,
        None => {
            // Task not found - might be a duplicate completion or old task
            println!(
                "Warning: task {} not found in active_tasks (completion from {})",
                req.task_id, req.worker_id
            );
            return;
        }
    };

    match task {
        ActiveTask::Scan { phenotype_id, partition_id: _, source } => {
            // Find the phenotype's pipeline state
            let state = match batch.active_phenotypes.get_mut(&phenotype_id) {
                Some(state) => state,
                None => {
                    println!(
                        "Warning: phenotype {} not found in active_phenotypes for scan completion",
                        phenotype_id
                    );
                    return;
                }
            };

            // Mark partitions as complete
            for &part_id in &req.partitions {
                match source {
                    ManhattanSource::Exome => {
                        if state.exome_processing.remove(&part_id).is_some() {
                            state.exome_completed.insert(part_id);
                        }
                    }
                    ManhattanSource::Genome => {
                        if state.genome_processing.remove(&part_id).is_some() {
                            state.genome_completed.insert(part_id);
                        }
                    }
                }
            }

            // Check if scan phase is complete for this phenotype
            let exome_done = state.exome_completed.len() == state.exome_total_partitions;
            let genome_done = state.genome_completed.len() == state.genome_total_partitions;
            let exome_idle = state.exome_pending.is_empty() && state.exome_processing.is_empty();
            let genome_idle = state.genome_pending.is_empty() && state.genome_processing.is_empty();

            if (exome_done || state.exome_total_partitions == 0)
                && (genome_done || state.genome_total_partitions == 0)
                && exome_idle
                && genome_idle
            {
                println!(
                    "Phenotype {} scan complete, moving to aggregate queue",
                    phenotype_id
                );

                // Build aggregate spec and move to ready_to_aggregate
                let original = &state.original_spec;
                let aggregate_spec = ManhattanAggregateSpec {
                    output_path: original.output_path.clone(),
                    exome_results: original.exome.clone(),
                    genome_results: original.genome.clone(),
                    gene_burden: original.gene_burden.clone(),
                    exome_annotations: original.exome_annotations.clone(),
                    genome_annotations: original.genome_annotations.clone(),
                    genes: original.genes.clone(),
                    threshold: original.threshold,
                    gene_threshold: original.gene_threshold,
                    locus_threshold: original.locus_threshold,
                    locus_window: original.locus_window,
                    locus_plots: original.locus_plots,
                    width: original.width,
                    height: original.height,
                    layout: state.layout.clone().unwrap_or_default(),
                    y_scale: state.y_scale.clone().unwrap_or_default(),
                    cleanup: false,
                };

                batch.ready_to_aggregate.push((phenotype_id.clone(), aggregate_spec));

                // Remove from active phenotypes
                batch.active_phenotypes.remove(&phenotype_id);
            } else {
                // Log progress
                let source_name = match source {
                    ManhattanSource::Exome => "exome",
                    ManhattanSource::Genome => "genome",
                };
                let (done, total) = match source {
                    ManhattanSource::Exome => (state.exome_completed.len(), state.exome_total_partitions),
                    ManhattanSource::Genome => (state.genome_completed.len(), state.genome_total_partitions),
                };
                println!(
                    "Phenotype {} {} progress: {}/{} partitions",
                    phenotype_id, source_name, done, total
                );
            }
        }

        ActiveTask::AggregateBatch { phenotype_ids } => {
            // Aggregate batch completed - mark all phenotypes as done
            for phenotype_id in phenotype_ids {
                batch.completed_count += 1;
                println!(
                    "Phenotype {} aggregate complete ({}/{})",
                    phenotype_id, batch.completed_count, batch.total_phenotypes
                );
            }
        }
    }
}

/// Handler for POST /complete - worker reports completion.
async fn complete_work(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<CompleteRequest>,
) -> axum::Json<CompleteResponse> {
    let mut data = state.lock().unwrap();

    // Check if this is a failure report
    if let Some(ref error) = req.error {
        // Log the error prominently
        println!(
            "ERROR from worker {}: partitions {:?} failed: {}",
            req.worker_id, req.partitions, error
        );

        // Store the error for dashboard display
        data.last_error = Some(format!(
            "Worker {} failed on partitions {:?}: {}",
            req.worker_id, req.partitions, error
        ));

        // Remove task from tracking
        data.active_tasks.remove(&req.task_id);

        // Remove from processing and add to failed
        for &part_id in &req.partitions {
            data.processing_partitions.remove(&part_id);
            data.failed_partitions.insert(part_id);
        }

        // For batch jobs, update the batch state
        if let Some(ref mut batch) = data.batch_state {
            // If this was a scan task, the partitions are tracked per-phenotype
            // For simplicity, we increment failed_count (the phenotype may retry or fail)
            batch.failed_count += 1;
        }

        // For Manhattan jobs, also update the manhattan state
        if let Some(ref mut manhattan) = data.manhattan_state {
            for &part_id in &req.partitions {
                manhattan.exome_processing.remove(&part_id);
                manhattan.genome_processing.remove(&part_id);
                // If this was the aggregate task, mark it failed
                if manhattan.aggregate_dispatched && !manhattan.aggregate_complete {
                    println!("Aggregate task failed - job cannot complete without fixing the error");
                }
            }
        }

        return axum::Json(CompleteResponse { acknowledged: true });
    }

    // Check if this is a batch Manhattan job
    if data.batch_state.is_some() {
        let mut batch = data.batch_state.take().unwrap();
        complete_batch_work(&mut data, &mut batch, &req);
        data.batch_state = Some(batch);
    } else if data.manhattan_state.is_some() {
        // Check if this is a single Manhattan pipeline job
        let mut manhattan = data.manhattan_state.take().unwrap();
        complete_manhattan_work(&mut manhattan, &req, &mut data.last_progress_time);
        data.manhattan_state = Some(manhattan);
    } else {
        // Standard job completion
        for &part_id in &req.partitions {
            if data.processing_partitions.remove(&part_id).is_some() {
                data.completed_partitions.insert(part_id);
                // Update progress timestamp (R3)
                data.last_progress_time = Instant::now();
            } else {
                // Partition wasn't in processing (maybe timed out and reassigned)
                // Still mark as complete if not already
                if !data.completed_partitions.contains(&part_id) {
                    println!(
                        "Warning: partition {} completed by {} but wasn't in processing map",
                        part_id, req.worker_id
                    );
                    data.completed_partitions.insert(part_id);
                }
            }
        }
    }

    data.total_rows += req.rows_processed;

    // Store result_json if present (for Summary/Validate jobs)
    if let Some(result) = req.result_json {
        data.aggregated_results.push(result);
    }

    // Update per-worker stats
    touch_worker(&mut data, &req.worker_id);
    if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
        w.total_rows += req.rows_processed;
        w.partitions_completed += req.partitions.len();
    }

    let total = data.config.total_partitions;
    let done = data.completed_partitions.len();

    // Log progress periodically
    if done % 10 == 0 || done == total {
        println!(
            "Progress: {}/{} partitions ({:.1}%), {} total rows",
            done,
            total,
            (done as f64 / total as f64) * 100.0,
            data.total_rows
        );
    }

    axum::Json(CompleteResponse { acknowledged: true })
}

/// Handler for GET /status - query job status.
async fn get_status(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<StatusResponse> {
    let data = state.lock().unwrap();

    // Check for batch Manhattan job
    let (pending, processing, completed, total, is_complete) = if let Some(ref batch) = data.batch_state {
        let total = batch.total_phenotypes;
        let completed = batch.completed_count;
        let pending = batch.pending_queue.len();
        let processing = batch.active_phenotypes.len() + batch.ready_to_aggregate.len();
        let is_complete = batch.pending_queue.is_empty()
            && batch.active_phenotypes.is_empty()
            && batch.ready_to_aggregate.is_empty()
            && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;
        (pending, processing, completed, total, is_complete)
    } else if let Some(ref m) = data.manhattan_state {
        // Check if this is a single Manhattan pipeline job
        let total_parts = m.exome_total_partitions + m.genome_total_partitions;
        let completed_parts = m.exome_completed.len() + m.genome_completed.len();
        let processing_parts = m.exome_processing.len() + m.genome_processing.len();
        let pending_parts = m.exome_pending.len() + m.genome_pending.len();

        // Add aggregate phase (+1 task)
        let total = total_parts + 1;
        let completed = completed_parts + if m.aggregate_complete { 1 } else { 0 };
        let processing = processing_parts + if m.aggregate_dispatched && !m.aggregate_complete { 1 } else { 0 };
        let pending = pending_parts + if !m.aggregate_dispatched && m.phase == ManhattanPhase::Aggregate { 1 } else { 0 };
        let is_complete = m.phase == ManhattanPhase::Complete;

        (pending, processing, completed, total, is_complete)
    } else {
        let failed = data.failed_partitions.len();
        let completed = data.completed_partitions.len();
        let is_complete = (completed + failed) == data.config.total_partitions;
        (
            data.pending_partitions.len(),
            data.processing_partitions.len(),
            completed,
            data.config.total_partitions,
            is_complete,
        )
    };

    let failed = data.failed_partitions.len();

    axum::Json(StatusResponse {
        pending,
        processing,
        completed,
        total,
        total_rows: data.total_rows,
        failed,
        is_complete,
    })
}

/// Check worker liveness based on heartbeat timestamps.
fn check_worker_liveness(state: &SharedState) {
    let mut data = state.lock().unwrap();
    let now = Instant::now();
    let timeout = Duration::from_secs(WORKER_SUSPECT_TIMEOUT_SECS);

    for (worker_id, worker) in data.worker_registry.iter_mut() {
        if now.duration_since(worker.last_seen) > timeout
            && worker.status != WorkerStatus::SuspectedDead
        {
            println!("Worker {} not seen for >{}s, marking as suspected dead", worker_id, WORKER_SUSPECT_TIMEOUT_SECS);
            worker.status = WorkerStatus::SuspectedDead;
        }
    }
}

/// Handler for POST /heartbeat - worker sends telemetry.
async fn handle_heartbeat(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<HeartbeatRequest>,
) -> axum::Json<HeartbeatResponse> {
    let mut data = state.lock().unwrap();
    touch_worker(&mut data, &req.worker_id);

    if let Some(w) = data.worker_registry.get_mut(&req.worker_id) {
        // Revive if previously suspected dead
        if w.status == WorkerStatus::SuspectedDead {
            w.status = WorkerStatus::Active;
        }
        // Store telemetry snapshot in memory (for quick access to latest)
        w.metrics_history.push_back(req.telemetry.clone());
        if w.metrics_history.len() > MAX_METRICS_HISTORY {
            w.metrics_history.pop_front();
        }
    }

    // Persist to SQLite (fire-and-forget, don't block on DB errors)
    if let Err(e) = data.metrics_db.insert_snapshot(&req.worker_id, &req.telemetry) {
        eprintln!("Warning: failed to persist metrics to DB: {}", e);
    }

    axum::Json(HeartbeatResponse { acknowledged: true })
}

/// Handler for POST /api/job - submit a job to an idle coordinator.
async fn submit_job(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<JobConfigRequest>,
) -> axum::Json<JobConfigResponse> {
    let mut data = state.lock().unwrap();

    // R1: Check if workers are available
    let active_workers = data
        .worker_registry
        .values()
        .filter(|w| w.status != WorkerStatus::SuspectedDead)
        .count();

    // Allow if we have workers OR if force is used (force bypasses worker check too for testing)
    if active_workers == 0 && !req.force {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some(
                "No active workers connected. Scale up workers first or use --force.".to_string(),
            ),
        });
    }

    // R4: Handle running jobs
    if !data.idle {
        if !req.force {
            return axum::Json(JobConfigResponse {
                acknowledged: false,
                error: Some(
                    "Coordinator already has a job running. Use --force to supersede.".to_string(),
                ),
            });
        }
        println!("Superseding running job (--force)...");
        // Clear existing state will happen below
    }

    // Validate request
    // ManhattanBatch jobs don't use total_partitions (they manage partitions per-phenotype)
    let is_batch_job = matches!(&req.job_spec, JobSpec::ManhattanBatch { .. });
    if req.total_partitions == 0 && !is_batch_job {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some("total_partitions must be greater than 0".to_string()),
        });
    }

    // ManhattanBatch and Manhattan jobs don't require input_path (they use per-spec paths)
    let needs_input_path = !is_batch_job && !matches!(&req.job_spec, JobSpec::Manhattan(_));
    if req.input_path.is_empty() && needs_input_path {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some("input_path is required".to_string()),
        });
    }

    // Configure the job
    data.config.input_path = req.input_path.clone();
    data.config.job_spec = Some(req.job_spec.clone());
    data.config.total_partitions = req.total_partitions;
    data.config.filters = req.filters.clone();
    data.config.intervals = req.intervals.clone();
    if let Some(batch_size) = req.batch_size {
        data.config.batch_size = batch_size;
    }

    // Fill pending queue with partition indices
    data.pending_partitions = (0..req.total_partitions).collect();

    // Reset job state
    data.completed_partitions.clear();
    data.processing_partitions.clear();
    data.failed_partitions.clear();
    data.retry_counts.clear();
    data.total_rows = 0;
    data.job_start_time = Instant::now();
    data.last_progress_time = Instant::now();
    data.aggregated_results.clear();
    data.manhattan_state = None;
    data.batch_state = None;
    data.active_tasks.clear();

    // Don't clear worker registry on resubmission/superseding, as we want to keep connected workers
    // Only clear if this is a fresh start and we want to purge stale workers
    // data.worker_registry.clear();

    // Clear metrics database for new job
    if let Err(e) = data.metrics_db.clear() {
        eprintln!("Warning: failed to clear metrics DB: {}", e);
    }

    // Mark as no longer idle
    data.idle = false;

    // Handle ManhattanBatch jobs (batch scheduling mode)
    if let JobSpec::ManhattanBatch { specs: ref specs } = req.job_spec {
        let total_phenotypes = specs.len();

        // Clear standard partition tracking - batch mode uses its own
        data.pending_partitions.clear();

        println!(
            "Initializing Manhattan batch: {} phenotypes (lazy loading, max {} active)",
            total_phenotypes,
            BATCH_ACTIVE_LIMIT
        );

        data.batch_state = Some(BatchState {
            pending_queue: specs.iter().cloned().collect(),
            active_phenotypes: HashMap::new(),
            ready_to_aggregate: Vec::new(),
            completed_count: 0,
            failed_count: 0,
            total_phenotypes,
        });

        let output_desc = specs.first()
            .map(|s| s.output_path.clone())
            .unwrap_or_else(|| "(no output)".to_string());
        println!(
            "Job submitted: {} ({} phenotypes, output={})",
            req.job_spec.description(),
            total_phenotypes,
            output_desc
        );

        return axum::Json(JobConfigResponse {
            acknowledged: true,
            error: None,
        });
    }

    // Initialize Manhattan pipeline state if this is a single Manhattan job
    data.manhattan_state = if let JobSpec::Manhattan(ref spec) = req.job_spec {
        // For Manhattan jobs, we manage exome/genome partitions separately
        // Use partition counts from spec if available, otherwise fall back to total_partitions
        let exome_partitions = spec.exome_partitions.unwrap_or_else(|| {
            if spec.exome.is_some() { req.total_partitions } else { 0 }
        });
        let genome_partitions = spec.genome_partitions.unwrap_or_else(|| {
            if spec.genome.is_some() && spec.exome.is_none() { req.total_partitions } else { 0 }
        });

        // Clear the standard partition tracking since Manhattan uses its own
        data.pending_partitions.clear();

        println!(
            "Initializing Manhattan pipeline: {} exome partitions, {} genome partitions",
            exome_partitions, genome_partitions
        );

        Some(ManhattanPipelineState {
            phase: ManhattanPhase::Scan,
            original_spec: spec.clone(),
            layout: spec.layout.clone(),
            y_scale: spec.y_scale.clone(),
            exome_total_partitions: exome_partitions,
            exome_pending: (0..exome_partitions).collect(),
            exome_processing: HashMap::new(),
            exome_completed: HashSet::new(),
            genome_total_partitions: genome_partitions,
            genome_pending: (0..genome_partitions).collect(),
            genome_processing: HashMap::new(),
            genome_completed: HashSet::new(),
            aggregate_dispatched: false,
            aggregate_complete: false,
        })
    } else {
        None
    };

    let output_desc = req
        .job_spec
        .output_path()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "(no output)".to_string());
    println!(
        "Job submitted: {} ({} partitions, input={}, output={})",
        req.job_spec.description(),
        req.total_partitions,
        req.input_path,
        output_desc
    );

    axum::Json(JobConfigResponse {
        acknowledged: true,
        error: None,
    })
}

/// Handler for POST /api/cancel - cancel the running job.
async fn cancel_job(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<CancelRequest>,
) -> axum::Json<CancelResponse> {
    let mut data = state.lock().unwrap();

    if data.idle {
        return axum::Json(CancelResponse {
            success: false,
            message: "No job is currently running".to_string(),
        });
    }

    // Reset job state
    data.pending_partitions.clear();
    data.processing_partitions.clear();
    data.manhattan_state = None;
    data.batch_state = None;
    data.active_tasks.clear();
    data.idle = true;

    let reason = req.reason.unwrap_or_else(|| "User request".to_string());
    println!("Job cancelled: {}", reason);

    axum::Json(CancelResponse {
        success: true,
        message: format!("Job cancelled: {}", reason),
    })
}

/// Handler for GET /api/result - retrieve aggregated job results.
///
/// For Summary and Validate jobs, this returns the collected results from all workers.
async fn get_job_result(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<JobResultResponse> {
    let data = state.lock().unwrap();

    // Check if the job is complete
    let failed = data.failed_partitions.len();
    let completed = data.completed_partitions.len();
    let total = data.config.total_partitions;
    let is_complete = total > 0 && (completed + failed) == total;

    if data.idle {
        return axum::Json(JobResultResponse {
            available: false,
            result: None,
            error: Some("No job is running".to_string()),
        });
    }

    if !is_complete {
        return axum::Json(JobResultResponse {
            available: false,
            result: None,
            error: Some(format!(
                "Job not complete: {}/{} partitions done",
                completed, total
            )),
        });
    }

    // Return aggregated results as a JSON array
    axum::Json(JobResultResponse {
        available: true,
        result: Some(serde_json::Value::Array(data.aggregated_results.clone())),
        error: None,
    })
}

/// Handler for GET /api/dashboard/summary - overall job summary.
async fn get_dashboard_summary(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<DashboardSummary> {
    let data = state.lock().unwrap();

    let elapsed = data.job_start_time.elapsed().as_secs_f64();
    let failed = data.failed_partitions.len();

    // Check for batch Manhattan job (multi-phenotype mode)
    let (completed, processing, pending, total, is_complete) = if let Some(ref batch) = data.batch_state {
        // For batch jobs, track phenotype-level progress
        let total = batch.total_phenotypes;
        let completed = batch.completed_count;

        // Count active phenotypes (currently being scanned)
        let active_count = batch.active_phenotypes.len();

        // Count phenotypes ready to aggregate
        let ready_count = batch.ready_to_aggregate.len();

        // Pending = queued + active + ready (not yet fully complete)
        // Processing = active phenotypes being scanned
        let pending = batch.pending_queue.len();
        let processing = active_count + ready_count;

        let is_complete = batch.pending_queue.is_empty()
            && batch.active_phenotypes.is_empty()
            && batch.ready_to_aggregate.is_empty()
            && (batch.completed_count + batch.failed_count) == batch.total_phenotypes;

        (completed, processing, pending, total, is_complete)
    } else if let Some(ref m) = data.manhattan_state {
        // Check if this is a single Manhattan pipeline job
        let total_parts = m.exome_total_partitions + m.genome_total_partitions;
        let completed_parts = m.exome_completed.len() + m.genome_completed.len();
        let processing_parts = m.exome_processing.len() + m.genome_processing.len();
        let pending_parts = m.exome_pending.len() + m.genome_pending.len();

        // Add aggregate phase (+1 task)
        let total = total_parts + 1;
        let completed = completed_parts + if m.aggregate_complete { 1 } else { 0 };
        let processing = processing_parts + if m.aggregate_dispatched && !m.aggregate_complete { 1 } else { 0 };
        let pending = pending_parts + if !m.aggregate_dispatched && m.phase == ManhattanPhase::Aggregate { 1 } else { 0 };
        let is_complete = m.phase == ManhattanPhase::Complete;

        (completed, processing, pending, total, is_complete)
    } else {
        let completed = data.completed_partitions.len();
        let is_complete = (completed + failed) == data.config.total_partitions;
        (
            completed,
            data.processing_partitions.len(),
            data.pending_partitions.len(),
            data.config.total_partitions,
            is_complete,
        )
    };

    let progress_percent = if total > 0 {
        (completed as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    // Compute cluster-wide rows/sec from worker telemetry
    let cluster_rows_per_sec: f64 = data
        .worker_registry
        .values()
        .filter(|w| w.status == WorkerStatus::Active)
        .filter_map(|w| w.metrics_history.back())
        .map(|s| s.rows_per_sec)
        .sum();

    // ETA based on partition completion rate
    let eta_secs = if completed > 0 && !is_complete {
        let remaining = total - completed - failed;
        let secs_per_partition = elapsed / completed as f64;
        Some(remaining as f64 * secs_per_partition)
    } else {
        None
    };

    axum::Json(DashboardSummary {
        progress_percent,
        total_partitions: total,
        completed_partitions: completed,
        processing_partitions: processing,
        pending_partitions: pending,
        failed_partitions: failed,
        total_rows: data.total_rows,
        cluster_rows_per_sec,
        elapsed_secs: elapsed,
        eta_secs,
        is_complete,
        input_path: data.config.input_path.clone(),
        job_spec: data.config.job_spec.clone(),
        idle: data.idle,
        last_error: data.last_error.clone(),
    })
}

/// Handler for GET /api/dashboard/workers - list all workers.
async fn get_dashboard_workers(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<Vec<DashboardWorker>> {
    let data = state.lock().unwrap();
    let now = Instant::now();

    let workers: Vec<DashboardWorker> = data
        .worker_registry
        .iter()
        .map(|(id, w)| DashboardWorker {
            worker_id: id.clone(),
            status: w.status.as_str().to_string(),
            last_seen_secs: now.duration_since(w.last_seen).as_secs_f64(),
            latest: w.metrics_history.back().cloned(),
            total_rows: w.total_rows,
            partitions_completed: w.partitions_completed,
        })
        .collect();

    axum::Json(workers)
}

/// Handler for GET /api/dashboard/metrics - time-series metrics for charts.
async fn get_dashboard_metrics(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<DashboardMetrics> {
    let data = state.lock().unwrap();

    // Get all worker IDs from the database (includes historical workers)
    let worker_ids = match data.metrics_db.get_worker_ids() {
        Ok(ids) => ids,
        Err(e) => {
            eprintln!("Warning: failed to get worker IDs from DB: {}", e);
            // Fall back to in-memory registry
            data.worker_registry.keys().cloned().collect()
        }
    };

    let workers: Vec<WorkerMetricsSeries> = worker_ids
        .iter()
        .map(|id| {
            // Query all snapshots from DB for this worker
            let snapshots = data
                .metrics_db
                .get_worker_snapshots(id)
                .unwrap_or_else(|_| Vec::new());

            WorkerMetricsSeries {
                worker_id: id.clone(),
                snapshots,
            }
        })
        .collect();

    axum::Json(DashboardMetrics { workers })
}

/// Handler for GET /dashboard - serve the embedded dashboard HTML.
async fn serve_dashboard() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("../../static/dashboard.html"))
}

/// Handler for GET /api/binary - serve the hail-decoder binary.
///
/// This endpoint allows workers to download the hail-decoder binary directly
/// from the coordinator over the fast GCP internal network, instead of each
/// worker receiving it via slow SCP from the client machine.
///
/// We serve from the fixed install path rather than current_exe() because:
/// - current_exe() returns /proc/self/exe which becomes stale when binary is replaced
/// - The fixed path always points to the latest uploaded binary
const BINARY_INSTALL_PATH: &str = "/usr/local/bin/hail-decoder";

async fn serve_binary() -> impl axum::response::IntoResponse {
    use axum::http::{header, StatusCode};
    use axum::response::Response;
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;

    // Use the fixed install path - this always points to the latest binary
    // even after updates (unlike /proc/self/exe which becomes "(deleted)")
    let exe_path = Path::new(BINARY_INSTALL_PATH);

    // Read the file synchronously (it's a one-time operation per request)
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<(Vec<u8>, u64)> {
        let mut file = File::open(exe_path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let mut buffer = Vec::with_capacity(file_size as usize);
        file.read_to_end(&mut buffer)?;
        Ok((buffer, file_size))
    })
    .await;

    match result {
        Ok(Ok((buffer, file_size))) => {
            println!(
                "Serving binary {} ({} bytes) to worker",
                BINARY_INSTALL_PATH, file_size
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, file_size)
                .header(
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"hail-decoder\"",
                )
                .body(Body::from(buffer))
                .unwrap()
        }
        Ok(Err(e)) => {
            eprintln!("Failed to read binary file {}: {}", BINARY_INSTALL_PATH, e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to read binary: {}", e)))
                .unwrap()
        }
        Err(e) => {
            eprintln!("Task panicked reading binary: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Internal error: {}", e)))
                .unwrap()
        }
    }
}

/// Handler for POST /api/export-metrics - export metrics database to GCS.
///
/// This endpoint reads the SQLite metrics database and uploads it to a GCS path.
/// Used by `pool destroy --metrics-bucket` to save metrics before deleting VMs.
async fn export_metrics(
    axum::Json(req): axum::Json<ExportMetricsRequest>,
) -> axum::Json<ExportMetricsResponse> {
    use crate::io::CloudWriter;
    use std::io::Write;

    const METRICS_DB_PATH: &str = "/tmp/hail-coordinator-metrics.db";

    // Read the metrics database file
    let db_contents = match std::fs::read(METRICS_DB_PATH) {
        Ok(contents) => contents,
        Err(e) => {
            return axum::Json(ExportMetricsResponse {
                success: false,
                path: None,
                error: Some(format!("Failed to read metrics database: {}", e)),
            });
        }
    };

    if db_contents.is_empty() {
        return axum::Json(ExportMetricsResponse {
            success: false,
            path: None,
            error: Some("Metrics database is empty".to_string()),
        });
    }

    // Create cloud writer and upload
    let destination = req.destination.trim_end_matches('/');
    let upload_path = if destination.ends_with(".db") {
        destination.to_string()
    } else {
        format!("{}/metrics.db", destination)
    };

    let mut writer = match CloudWriter::new(&upload_path) {
        Ok(w) => w,
        Err(e) => {
            return axum::Json(ExportMetricsResponse {
                success: false,
                path: None,
                error: Some(format!("Failed to create cloud writer: {}", e)),
            });
        }
    };

    if let Err(e) = writer.write_all(&db_contents) {
        return axum::Json(ExportMetricsResponse {
            success: false,
            path: None,
            error: Some(format!("Failed to write metrics data: {}", e)),
        });
    }

    match writer.finish() {
        Ok(bytes) => {
            println!(
                "Exported metrics database ({} bytes) to {}",
                bytes, upload_path
            );
            axum::Json(ExportMetricsResponse {
                success: true,
                path: Some(upload_path),
                error: None,
            })
        }
        Err(e) => axum::Json(ExportMetricsResponse {
            success: false,
            path: None,
            error: Some(format!("Failed to upload metrics: {}", e)),
        }),
    }
}

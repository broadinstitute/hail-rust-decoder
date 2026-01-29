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
    HeartbeatRequest, HeartbeatResponse, JobConfigRequest, JobConfigResponse, StatusResponse,
    TelemetrySnapshot, WorkRequest, WorkResponse, WorkerMetricsSeries,
};
use crate::distributed::metrics_db::MetricsDb;
use crate::Result;
use axum::body::Body;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_util::io::ReaderStream;

/// Configuration for the coordinator.
pub struct CoordinatorConfig {
    /// Port to listen on
    pub port: u16,
    /// Path to input Hail table
    pub input_path: String,
    /// Path to output directory
    pub output_path: String,
    /// Total number of partitions to process
    pub total_partitions: usize,
    /// Number of partitions to assign per work request (batching)
    pub batch_size: usize,
    /// Timeout before rescheduling work (seconds)
    pub timeout_secs: u64,
    /// Timeout for stuck jobs making no progress (seconds)
    pub stuck_timeout_secs: u64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            input_path: String::new(),
            output_path: String::new(),
            total_partitions: 0,
            batch_size: 1, // Default to 1 partition per request for fine-grained retry
            timeout_secs: 600, // 10 minutes
            stuck_timeout_secs: 600, // 10 minutes for stuck job detection
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
}

type SharedState = Arc<Mutex<CoordinatorData>>;

/// Start the coordinator server using a config struct.
///
/// This function blocks until the job is complete or the server is interrupted.
pub async fn start_coordinator(config: CoordinatorConfig) -> Result<()> {
    run_coordinator(
        config.port,
        config.input_path,
        config.output_path,
        config.total_partitions,
        config.batch_size,
        config.timeout_secs,
    )
    .await
}

/// Properly structured coordinator startup.
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

    if idle {
        println!(
            "Coordinator starting on port {} in IDLE mode (waiting for job submission)",
            port
        );
        println!("  Submit a job via POST /api/job or pool submit --distributed");
    } else {
        println!(
            "Coordinator starting on port {} with {} partitions",
            port, total_partitions
        );
        println!("  Input: {}", input_path);
        println!("  Output: {}", output_path);
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
            output_path,
            total_partitions,
            batch_size,
            timeout_secs,
            stuck_timeout_secs: 600, // Default 10 minutes
        },
        total_rows: 0,
        retry_counts: HashMap::new(),
        failed_partitions: HashSet::new(),
        worker_registry: HashMap::new(),
        job_start_time: Instant::now(),
        last_progress_time: Instant::now(),
        idle,
        metrics_db,
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
            let (pending, processing, completed, failed, total, rows, is_idle) = {
                let data = monitor_state.lock().unwrap();
                (
                    data.pending_partitions.len(),
                    data.processing_partitions.len(),
                    data.completed_partitions.len(),
                    data.failed_partitions.len(),
                    data.config.total_partitions,
                    data.total_rows,
                    data.idle,
                )
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

            // Check if job is complete (all partitions either completed or failed)
            if total > 0 && (completed + failed) == total && processing == 0 && pending == 0 {
                if failed > 0 {
                    println!(
                        "Job finished with {} failed partitions out of {}. Total rows: {}",
                        failed, total, rows
                    );
                } else {
                    println!("All {} partitions completed! Total rows: {}", total, rows);
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
    if !data.completed_partitions.is_empty() {
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

    // Check if there's pending work
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

        axum::Json(WorkResponse::Task {
            partitions,
            input_path: data.config.input_path.clone(),
            output_path: data.config.output_path.clone(),
            total_partitions: data.config.total_partitions,
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

/// Handler for POST /complete - worker reports completion.
async fn complete_work(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<CompleteRequest>,
) -> axum::Json<CompleteResponse> {
    let mut data = state.lock().unwrap();

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

    data.total_rows += req.rows_processed;

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

    let failed = data.failed_partitions.len();
    let completed = data.completed_partitions.len();
    let is_complete = (completed + failed) == data.config.total_partitions;

    axum::Json(StatusResponse {
        pending: data.pending_partitions.len(),
        processing: data.processing_partitions.len(),
        completed,
        total: data.config.total_partitions,
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
    if req.total_partitions == 0 {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some("total_partitions must be greater than 0".to_string()),
        });
    }

    if req.input_path.is_empty() {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some("input_path is required".to_string()),
        });
    }

    if req.output_path.is_empty() {
        return axum::Json(JobConfigResponse {
            acknowledged: false,
            error: Some("output_path is required".to_string()),
        });
    }

    // Configure the job
    data.config.input_path = req.input_path.clone();
    data.config.output_path = req.output_path.clone();
    data.config.total_partitions = req.total_partitions;
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

    // Don't clear worker registry on resubmission/superseding, as we want to keep connected workers
    // Only clear if this is a fresh start and we want to purge stale workers
    // data.worker_registry.clear();

    // Clear metrics database for new job
    if let Err(e) = data.metrics_db.clear() {
        eprintln!("Warning: failed to clear metrics DB: {}", e);
    }

    // Mark as no longer idle
    data.idle = false;

    println!(
        "Job submitted: {} partitions, input={}, output={}",
        req.total_partitions, req.input_path, req.output_path
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
    data.idle = true;

    let reason = req.reason.unwrap_or_else(|| "User request".to_string());
    println!("Job cancelled: {}", reason);

    axum::Json(CancelResponse {
        success: true,
        message: format!("Job cancelled: {}", reason),
    })
}

/// Handler for GET /api/dashboard/summary - overall job summary.
async fn get_dashboard_summary(
    axum::extract::State(state): axum::extract::State<SharedState>,
) -> axum::Json<DashboardSummary> {
    let data = state.lock().unwrap();

    let completed = data.completed_partitions.len();
    let failed = data.failed_partitions.len();
    let total = data.config.total_partitions;
    let elapsed = data.job_start_time.elapsed().as_secs_f64();
    let is_complete = (completed + failed) == total;

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
        processing_partitions: data.processing_partitions.len(),
        pending_partitions: data.pending_partitions.len(),
        failed_partitions: failed,
        total_rows: data.total_rows,
        cluster_rows_per_sec,
        elapsed_secs: elapsed,
        eta_secs,
        is_complete,
        input_path: data.config.input_path.clone(),
        output_path: data.config.output_path.clone(),
        idle: data.idle,
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
    use std::path::Path;

    // Use the fixed install path - this always points to the latest binary
    // even after updates (unlike /proc/self/exe which becomes "(deleted)")
    let exe_path = Path::new(BINARY_INSTALL_PATH);

    // Open the file
    let file = match tokio::fs::File::open(&exe_path).await {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open binary file {}: {}", exe_path.display(), e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to open binary: {}", e)))
                .unwrap();
        }
    };

    // Get file metadata for Content-Length
    let metadata = match file.metadata().await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Failed to get binary metadata: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to read binary metadata: {}", e)))
                .unwrap();
        }
    };

    let file_size = metadata.len();

    // Create a stream from the file
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    println!(
        "Serving binary {} ({} bytes) to worker",
        exe_path.display(),
        file_size
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, file_size)
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"hail-decoder\"",
        )
        .body(body)
        .unwrap()
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

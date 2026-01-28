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
    CompleteRequest, CompleteResponse, StatusResponse, WorkRequest, WorkResponse,
};
use crate::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
        }
    }
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

    println!(
        "Coordinator starting on port {} with {} partitions",
        port, total_partitions
    );
    println!("  Input: {}", input_path);
    println!("  Output: {}", output_path);
    println!("  Batch size: {}", batch_size);
    println!("  Timeout: {}s", timeout_secs);

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
        },
        total_rows: 0,
        retry_counts: HashMap::new(),
        failed_partitions: HashSet::new(),
    }));

    // Start background timeout monitor
    let monitor_state = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            check_timeouts(&monitor_state, timeout_secs);

            // Print periodic status
            let (pending, processing, completed, failed, total, rows) = {
                let data = monitor_state.lock().unwrap();
                (
                    data.pending_partitions.len(),
                    data.processing_partitions.len(),
                    data.completed_partitions.len(),
                    data.failed_partitions.len(),
                    data.config.total_partitions,
                    data.total_rows,
                )
            };

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
            if (completed + failed) == total && processing == 0 && pending == 0 {
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
        .with_state(state);

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

/// Handler for POST /work - worker requests work.
async fn get_work(
    axum::extract::State(state): axum::extract::State<SharedState>,
    axum::Json(req): axum::Json<WorkRequest>,
) -> axum::Json<WorkResponse> {
    let mut data = state.lock().unwrap();

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
        axum::Json(WorkResponse::Wait)
    } else {
        // All done
        println!("Worker {} requested work, but all partitions complete", req.worker_id);
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

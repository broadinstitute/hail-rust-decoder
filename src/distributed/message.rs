//! Message types for Coordinator/Worker communication.
//!
//! These messages are exchanged over HTTP as JSON.

use serde::{Deserialize, Serialize};

/// Request from a worker asking for work.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkRequest {
    /// Unique identifier for this worker
    pub worker_id: String,
}

/// Response from coordinator with work assignment.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WorkResponse {
    /// Work is available - process these partitions
    #[serde(rename = "task")]
    Task {
        /// Partition indices to process
        partitions: Vec<usize>,
        /// Path to input Hail table
        input_path: String,
        /// Path to output directory
        output_path: String,
        /// Total number of partitions in the table (for output file naming)
        total_partitions: usize,
    },
    /// No work available but job is still in progress - wait and retry
    #[serde(rename = "wait")]
    Wait,
    /// All work is complete - worker should exit
    #[serde(rename = "exit")]
    Exit,
}

/// Request from a worker reporting completion.
#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteRequest {
    /// Worker that completed the work
    pub worker_id: String,
    /// Partitions that were completed
    pub partitions: Vec<usize>,
    /// Number of rows processed
    pub rows_processed: usize,
}

/// Response to completion request.
#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteResponse {
    /// Whether the completion was acknowledged
    pub acknowledged: bool,
}

/// Status query response from coordinator.
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Number of partitions pending
    pub pending: usize,
    /// Number of partitions currently being processed
    pub processing: usize,
    /// Number of partitions completed
    pub completed: usize,
    /// Total partitions in the job
    pub total: usize,
    /// Total rows processed so far
    pub total_rows: usize,
    /// Number of partitions that permanently failed (max retries exceeded)
    pub failed: usize,
    /// Whether the job is complete
    pub is_complete: bool,
}

/// A point-in-time telemetry snapshot from a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    /// Unix timestamp in milliseconds
    pub timestamp_ms: u64,
    /// CPU usage percentage (0-100), None if sysinfo unavailable
    pub cpu_percent: Option<f32>,
    /// Memory used in bytes, None if sysinfo unavailable
    pub memory_used_bytes: Option<u64>,
    /// Memory total in bytes, None if sysinfo unavailable
    pub memory_total_bytes: Option<u64>,
    /// Rows processed per second (computed by worker)
    pub rows_per_sec: f64,
    /// Total rows processed so far by this worker
    pub total_rows: usize,
    /// Currently active partition, if any
    pub active_partition: Option<usize>,
    /// Partitions completed by this worker
    pub partitions_completed: usize,

    // Extended metrics for btop-style dashboard

    /// Per-core CPU usage percentages (0-100 for each core)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_per_core: Option<Vec<f32>>,
    /// Disk read rate in bytes per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_read_bytes_sec: Option<f64>,
    /// Disk write rate in bytes per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes_sec: Option<f64>,
    /// Disk space used in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_used_bytes: Option<u64>,
    /// Disk space total in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_total_bytes: Option<u64>,
    /// Network receive rate in bytes per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_rx_bytes_sec: Option<f64>,
    /// Network transmit rate in bytes per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_tx_bytes_sec: Option<f64>,
    /// Cumulative network bytes received (for totals display)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_rx_total_bytes: Option<u64>,
    /// Cumulative network bytes transmitted (for totals display)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_tx_total_bytes: Option<u64>,
}

/// Heartbeat request from worker to coordinator.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// Worker sending the heartbeat
    pub worker_id: String,
    /// Current telemetry snapshot
    pub telemetry: TelemetrySnapshot,
}

/// Heartbeat response from coordinator.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Whether the heartbeat was acknowledged
    pub acknowledged: bool,
}

/// Dashboard summary for the overall job.
#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardSummary {
    /// Job progress percentage (0-100)
    pub progress_percent: f64,
    /// Total partitions in the job
    pub total_partitions: usize,
    /// Partitions completed
    pub completed_partitions: usize,
    /// Partitions currently processing
    pub processing_partitions: usize,
    /// Partitions pending
    pub pending_partitions: usize,
    /// Partitions permanently failed
    pub failed_partitions: usize,
    /// Total rows processed across all workers
    pub total_rows: usize,
    /// Aggregate rows per second across all workers
    pub cluster_rows_per_sec: f64,
    /// Job elapsed time in seconds
    pub elapsed_secs: f64,
    /// Estimated time remaining in seconds, if calculable
    pub eta_secs: Option<f64>,
    /// Whether the job is complete
    pub is_complete: bool,
    /// Input path being processed
    pub input_path: String,
    /// Output path
    pub output_path: String,
    /// Whether the coordinator is idle (waiting for job submission)
    #[serde(default)]
    pub idle: bool,
}

/// Request to submit a new job to an idle coordinator.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobConfigRequest {
    /// Path to input Hail table
    pub input_path: String,
    /// Path to output directory
    pub output_path: String,
    /// Total number of partitions to process
    pub total_partitions: usize,
    /// Number of partitions per work request (optional, defaults to coordinator's batch_size)
    #[serde(default)]
    pub batch_size: Option<usize>,
}

/// Response to a job submission request.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobConfigResponse {
    /// Whether the job was accepted
    pub acknowledged: bool,
    /// Error message if job was rejected
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to export metrics database to GCS.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportMetricsRequest {
    /// GCS path to upload the metrics database (e.g., gs://bucket/path/metrics.db)
    pub destination: String,
}

/// Response to metrics export request.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportMetricsResponse {
    /// Whether the export was successful
    pub success: bool,
    /// Path where metrics were uploaded
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Error message if export failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Dashboard info about a single worker.
#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardWorker {
    /// Worker identifier
    pub worker_id: String,
    /// Current status
    pub status: String,
    /// Seconds since last heartbeat
    pub last_seen_secs: f64,
    /// Latest telemetry snapshot
    pub latest: Option<TelemetrySnapshot>,
    /// Total rows reported by this worker
    pub total_rows: usize,
    /// Total partitions completed by this worker
    pub partitions_completed: usize,
}

/// Time-series metrics data for charts.
#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardMetrics {
    /// Per-worker time-series data
    pub workers: Vec<WorkerMetricsSeries>,
}

/// Time-series data for a single worker.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerMetricsSeries {
    /// Worker identifier
    pub worker_id: String,
    /// Telemetry snapshots (most recent last)
    pub snapshots: Vec<TelemetrySnapshot>,
}

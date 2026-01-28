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
    /// Whether the job is complete
    pub is_complete: bool,
}

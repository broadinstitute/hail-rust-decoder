//! Message types for Coordinator/Worker communication.
//!
//! These messages are exchanged over HTTP as JSON.

use serde::{Deserialize, Serialize};

/// Specification for a distributed job operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JobSpec {
    /// Export to Parquet files
    ExportParquet {
        output_path: String,
    },
    /// Export to JSON files (NDJSON)
    ExportJson {
        output_path: String,
        /// Optional field to group by (creates separate files/directories)
        #[serde(default)]
        group_by: Option<String>,
    },
    /// Compute summary statistics
    Summary,
    /// Validate against schema
    Validate {
        schema_path: String,
        #[serde(default)]
        fail_fast: bool,
    },
    /// Generate Manhattan plot (high-level submission - coordinator splits into phases)
    Manhattan(ManhattanSpec),
    /// Submit a batch of Manhattan plots (coordinator queues them)
    ManhattanBatch { specs: Vec<ManhattanSpec> },
    /// Phase 1: Worker scans partitions, outputs partial PNGs + sig.parquet
    ManhattanScan(ManhattanScanSpec),
    /// Phase 2: Single worker aggregates results, joins annotations, generates locus plots
    ManhattanAggregate(ManhattanAggregateSpec),
    /// Phase 2 (Batch): Worker executes multiple aggregation tasks in parallel
    ManhattanAggregateBatch { specs: Vec<ManhattanAggregateSpec> },
    /// Generate locus plots from existing Manhattan output
    Loci(LociSpec),
    /// Export to ClickHouse (requires clickhouse feature)
    ExportClickhouse {
        clickhouse_url: String,
        table_name: String,
    },
    /// Ingest Manhattan results into ClickHouse (Coordinator job)
    /// Coordinator scans input_dir, creates one task per phenotype
    IngestManhattan {
        input_dir: String,
        clickhouse_url: String,
        database: String,
    },
    /// Ingest a specific phenotype into ClickHouse (Worker task)
    IngestManhattanTask {
        phenotype_id: String,
        ancestry: String,
        /// Base path for this phenotype (e.g., gs://bucket/manhattans/meta/pheno_id/)
        base_path: String,
        clickhouse_url: String,
        database: String,
    },
}

/// Progress stats for batch Manhattan jobs (multi-phenotype mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardBatchProgress {
    pub total: usize,
    pub queued: usize,
    pub active_scan: usize,
    pub active_aggregate: usize,
    pub completed: usize,
    pub failed: usize,
}

/// Status of a single phenotype in a batch job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhenotypeStatus {
    pub id: String,
    /// Stage: "queued", "scanning", "aggregating", "completed", "failed"
    pub stage: String,
    pub partitions_done: usize,
    pub partitions_total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    /// Total duration in seconds (from scan start to completion)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<f64>,
    /// Accumulated CPU core-seconds for this phenotype
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_core_secs: Option<f64>,
}

/// Response containing status of all phenotypes in a batch.
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchStatusResponse {
    pub phenotypes: Vec<PhenotypeStatus>,
}

/// Configuration for distributed locus plot generation (coordinator-level).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LociSpec {
    /// Path to Manhattan output directory (contains *_significant.parquet)
    pub output_dir: String,
    /// Path to exome results Hail table (for reading variants)
    #[serde(default)]
    pub exome_results: Option<String>,
    /// Path to genome results Hail table (for reading variants)
    #[serde(default)]
    pub genome_results: Option<String>,
    /// Path to gene burden results Hail table (for seeding locus regions)
    #[serde(default)]
    pub gene_burden: Option<String>,
    /// Window size around significant hits
    #[serde(default = "default_locus_window")]
    pub locus_window: i32,
    /// Significance threshold for variants
    #[serde(default = "default_threshold")]
    pub threshold: f64,
    /// Significance threshold for gene burden
    #[serde(default = "default_gene_threshold")]
    pub gene_threshold: f64,
}

/// A locus region to generate a plot for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocusRegion {
    pub contig: String,
    pub start: i32,
    pub end: i32,
    /// Lead variant position (for manifest)
    pub lead_position: i32,
    /// Lead variant p-value
    pub lead_pvalue: f64,
    /// Source of lead variant (exome/genome)
    pub lead_source: String,
}

/// Worker-level locus generation job (assigned subset of regions).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LociWorkerSpec {
    /// Path to Manhattan output directory
    pub output_dir: String,
    /// Path to exome results Hail table
    #[serde(default)]
    pub exome_results: Option<String>,
    /// Path to genome results Hail table
    #[serde(default)]
    pub genome_results: Option<String>,
    /// Assigned locus regions to process
    pub regions: Vec<LocusRegion>,
    /// Significance threshold
    pub threshold: f64,
}

fn default_threshold() -> f64 {
    5e-8
}

/// Configuration for a distributed Manhattan plot job.
///
/// Matches the fields in `PipelineConfig` from `src/manhattan/pipeline.rs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManhattanSpec {
    // Identity metadata (for ClickHouse partitioning)
    /// Phenotype identifier (e.g., analysis ID)
    #[serde(default)]
    pub phenotype: Option<String>,
    /// Ancestry group (e.g., "meta", "EUR", "AFR")
    #[serde(default)]
    pub ancestry: Option<String>,

    // Data inputs
    /// Path to Exome results table
    #[serde(default)]
    pub exome: Option<String>,
    /// Path to Exome annotations table (for merge-join)
    #[serde(default)]
    pub exome_annotations: Option<String>,
    /// Path to Genome results table
    #[serde(default)]
    pub genome: Option<String>,
    /// Path to Genome annotations table (for merge-join)
    #[serde(default)]
    pub genome_annotations: Option<String>,
    /// Path to gene burden results table
    #[serde(default)]
    pub gene_burden: Option<String>,
    /// Path to gnomAD genes table
    #[serde(default)]
    pub genes: Option<String>,
    /// Path to exome expected p-values table (for QQ plots)
    #[serde(default)]
    pub exome_exp_p: Option<String>,
    /// Path to genome expected p-values table (for QQ plots)
    #[serde(default)]
    pub genome_exp_p: Option<String>,

    // Thresholds
    /// P-value threshold for significant variants (default: 5e-8)
    pub threshold: f64,
    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[serde(default = "default_gene_threshold")]
    pub gene_threshold: f64,
    /// P-value threshold to buffer variants for locus plots (default: 0.01)
    #[serde(default = "default_locus_threshold")]
    pub locus_threshold: f64,
    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[serde(default = "default_locus_window")]
    pub locus_window: i32,
    /// Generate locus-zoom style plots for significant regions
    #[serde(default)]
    pub locus_plots: bool,

    // Output
    /// Image width in pixels
    pub width: u32,
    /// Image height in pixels
    pub height: u32,
    /// Field name for P-value (Y-axis)
    pub y_field: String,
    /// Output path (directory)
    pub output_path: String,

    // Pre-computed layout (set by coordinator for distributed rendering)
    /// Chromosome layout for mapping genomic positions to pixel X coordinates
    #[serde(default)]
    pub layout: Option<crate::manhattan::layout::ChromosomeLayout>,
    /// Y-axis scale for mapping -log10(p) to pixel Y coordinates
    #[serde(default)]
    pub y_scale: Option<crate::manhattan::layout::YScale>,

    /// Skip automatic composite step (run manually later with --from-shards)
    #[serde(default)]
    pub skip_composite: bool,

    // Partition counts (set by pool submit for multi-table jobs)
    /// Number of partitions in exome results table
    #[serde(default)]
    pub exome_partitions: Option<usize>,
    /// Number of partitions in genome results table
    #[serde(default)]
    pub genome_partitions: Option<usize>,
}

fn default_gene_threshold() -> f64 {
    2.5e-6
}

fn default_locus_threshold() -> f64 {
    0.01
}

fn default_locus_window() -> i32 {
    1_000_000
}

impl ManhattanSpec {
    /// Get the primary input path for partition counting.
    ///
    /// Returns the first available of: exome, genome, or gene_burden.
    pub fn primary_input_path(&self) -> Option<&str> {
        self.exome
            .as_deref()
            .or(self.genome.as_deref())
            .or(self.gene_burden.as_deref())
    }
}

/// Data source indicator for Manhattan scan partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManhattanSource {
    Exome,
    Genome,
}

/// Phase 1: Scan job specification for a single data source.
///
/// Workers receive this spec to scan partitions and output:
/// - Partial PNG (transparent background, for later compositing)
/// - Significant hits Parquet (variants below threshold)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManhattanScanSpec {
    // Identity metadata (for significant hits Parquet)
    /// Phenotype identifier
    pub phenotype: String,
    /// Ancestry group
    pub ancestry: String,

    /// Which data source this scan is for
    pub source: ManhattanSource,
    /// Path to the results table (exome or genome)
    pub table_path: String,
    /// Output directory (will create {source}/part-*.png and {source}/part-*-sig.parquet)
    pub output_path: String,
    /// P-value threshold for significant hits (default: 5e-8)
    pub threshold: f64,
    /// Field name for P-value (Y-axis)
    pub y_field: String,

    // Pre-computed layout from coordinator
    /// Chromosome layout for mapping genomic positions to pixel X coordinates
    pub layout: crate::manhattan::layout::ChromosomeLayout,
    /// Y-axis scale for mapping -log10(p) to pixel Y coordinates
    pub y_scale: crate::manhattan::layout::YScale,
    /// Image width in pixels
    pub width: u32,
    /// Image height in pixels
    pub height: u32,
}

/// Phase 2: Aggregate job specification.
///
/// A single worker receives this to:
/// 1. Composite partial PNGs into final Manhattan plots
/// 2. Process gene burden table
/// 3. Merge significant hits from scan phase
/// 4. Generate locus plots for significant regions
/// 5. Write manifest.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManhattanAggregateSpec {
    /// Output directory (contains partial PNGs/parquets from scan phase)
    pub output_path: String,

    // Input tables for targeted reads (locus plots)
    /// Path to Exome results Hail table
    #[serde(default)]
    pub exome_results: Option<String>,
    /// Path to Genome results Hail table
    #[serde(default)]
    pub genome_results: Option<String>,
    /// Path to gene burden results Hail table
    #[serde(default)]
    pub gene_burden: Option<String>,
    /// Path to exome expected p-values table (for QQ plots)
    #[serde(default)]
    pub exome_exp_p: Option<String>,
    /// Path to genome expected p-values table (for QQ plots)
    #[serde(default)]
    pub genome_exp_p: Option<String>,

    // Annotation tables (original Hail tables - for locus plot annotation)
    /// Path to exome annotations Hail table
    #[serde(default)]
    pub exome_annotations: Option<String>,
    /// Path to genome annotations Hail table
    #[serde(default)]
    pub genome_annotations: Option<String>,
    /// Path to gnomAD genes Hail table
    #[serde(default)]
    pub genes: Option<String>,

    // Thresholds
    /// P-value threshold for significant variants (for reference)
    pub threshold: f64,
    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[serde(default = "default_gene_threshold")]
    pub gene_threshold: f64,
    /// P-value threshold for locus plot regions (default: 0.01)
    #[serde(default = "default_locus_threshold")]
    pub locus_threshold: f64,
    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[serde(default = "default_locus_window")]
    pub locus_window: i32,
    /// Generate locus-zoom style plots for significant regions
    #[serde(default)]
    pub locus_plots: bool,

    // Rendering config
    /// Image width in pixels
    pub width: u32,
    /// Image height in pixels
    pub height: u32,

    // Layout (same as scan phase)
    /// Chromosome layout for rendering gene burden manhattan
    pub layout: crate::manhattan::layout::ChromosomeLayout,
    /// Y-axis scale for rendering gene burden manhattan
    pub y_scale: crate::manhattan::layout::YScale,

    /// Delete intermediate shards after aggregation
    #[serde(default)]
    pub cleanup: bool,
}

impl JobSpec {
    /// Get a human-readable description of the job type.
    pub fn description(&self) -> &'static str {
        match self {
            JobSpec::ExportParquet { .. } => "export parquet",
            JobSpec::ExportJson { .. } => "export json",
            JobSpec::Summary => "summary",
            JobSpec::Validate { .. } => "validate",
            JobSpec::Manhattan(_) => "manhattan plot",
            JobSpec::ManhattanBatch { .. } => "manhattan batch",
            JobSpec::ManhattanScan(_) => "manhattan scan",
            JobSpec::ManhattanAggregate(_) => "manhattan aggregate",
            JobSpec::ManhattanAggregateBatch { .. } => "manhattan aggregate batch",
            JobSpec::Loci(_) => "loci plots",
            JobSpec::ExportClickhouse { .. } => "export clickhouse",
            JobSpec::IngestManhattan { .. } => "ingest manhattan",
            JobSpec::IngestManhattanTask { .. } => "ingest manhattan task",
        }
    }

    /// Get the output path if this job produces output.
    pub fn output_path(&self) -> Option<&str> {
        match self {
            JobSpec::ExportParquet { output_path } => Some(output_path),
            JobSpec::ExportJson { output_path, .. } => Some(output_path),
            JobSpec::Summary => None,
            JobSpec::Validate { .. } => None,
            JobSpec::Manhattan(spec) => Some(&spec.output_path),
            JobSpec::ManhattanBatch { specs } => specs.first().map(|s| s.output_path.as_str()),
            JobSpec::ManhattanScan(spec) => Some(&spec.output_path),
            JobSpec::ManhattanAggregate(spec) => Some(&spec.output_path),
            JobSpec::ManhattanAggregateBatch { specs } => specs.first().map(|s| s.output_path.as_str()),
            JobSpec::Loci(spec) => Some(&spec.output_dir),
            JobSpec::ExportClickhouse { table_name, .. } => Some(table_name),
            JobSpec::IngestManhattan { input_dir, .. } => Some(input_dir),
            JobSpec::IngestManhattanTask { base_path, .. } => Some(base_path),
        }
    }
}

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
        /// Unique task identifier (for tracking in batch mode)
        #[serde(default)]
        task_id: String,
        /// Partition indices to process
        partitions: Vec<usize>,
        /// Path to input Hail table
        input_path: String,
        /// Job specification (what to do with the data)
        job_spec: JobSpec,
        /// Total number of partitions in the table (for output file naming)
        total_partitions: usize,
        /// Filter conditions (where clauses)
        #[serde(default)]
        filters: Vec<String>,
        /// Interval filters
        #[serde(default)]
        intervals: Vec<String>,
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
    /// Unique task identifier (matches WorkResponse)
    #[serde(default)]
    pub task_id: String,
    /// Partitions that were completed (or failed)
    pub partitions: Vec<usize>,
    /// Number of rows processed
    pub rows_processed: usize,
    /// Optional result data for aggregation (e.g., stats, validation report)
    #[serde(default)]
    pub result_json: Option<serde_json::Value>,
    /// Error message if the task failed (None = success)
    #[serde(default)]
    pub error: Option<String>,
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
    /// Job specification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_spec: Option<JobSpec>,
    /// Whether the coordinator is idle (waiting for job submission)
    #[serde(default)]
    pub idle: bool,
    /// Last error message (if any task failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Batch progress stats (for batch Manhattan jobs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_progress: Option<DashboardBatchProgress>,
    /// Build version (git commit hash)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_version: Option<String>,
}

/// Request to submit a new job to an idle coordinator.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobConfigRequest {
    /// Path to input Hail table
    pub input_path: String,
    /// Job specification
    pub job_spec: JobSpec,
    /// Total number of partitions to process
    pub total_partitions: usize,
    /// Number of partitions per work request (optional, defaults to coordinator's batch_size)
    #[serde(default)]
    pub batch_size: Option<usize>,
    /// Force submission even if a job is already running (supersede)
    #[serde(default)]
    pub force: bool,
    /// Filter conditions (where clauses)
    #[serde(default)]
    pub filters: Vec<String>,
    /// Interval filters
    #[serde(default)]
    pub intervals: Vec<String>,
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

/// Request to cancel the current job.
#[derive(Debug, Serialize, Deserialize)]
pub struct CancelRequest {
    /// Optional reason for cancellation
    pub reason: Option<String>,
}

/// Response to cancellation request.
#[derive(Debug, Serialize, Deserialize)]
pub struct CancelResponse {
    /// Whether cancellation was successful
    pub success: bool,
    /// Status message
    pub message: String,
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

/// Response containing aggregated job results.
///
/// Used by `GET /api/result` to retrieve final stats for summary/validate jobs.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobResultResponse {
    /// Whether a result is available
    pub available: bool,
    /// The aggregated result (structure depends on job type)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// Error message if result retrieval failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

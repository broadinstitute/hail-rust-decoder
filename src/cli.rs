//! CLI argument definitions using clap derive API.

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "hail-decoder",
    version,
    about = "Hail Table Decoder and Converter",
    long_about = None
)]
pub struct Cli {
    /// Path to configuration file (default: ~/.config/hail-decoder/config.toml)
    #[arg(long, global = true)]
    pub config: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Show table metadata, keys, partition layout, and schema (fast)
    Info {
        /// Path to the Hail table or VCF file
        path: String,
    },

    /// Scan full dataset to calculate row counts and field statistics (slow)
    Summary {
        /// Path to the Hail table
        path: String,
    },

    /// Stream rows with optional filtering (lazy)
    Query(QueryArgs),

    /// Export data to other formats
    Export {
        #[command(subcommand)]
        command: ExportCommands,
    },

    /// Schema operations (validate, generate)
    #[cfg(feature = "validation")]
    Schema {
        #[command(subcommand)]
        command: SchemaSubcommands,
    },

    /// Generate Manhattan plots (PNG + JSON sidecar)
    Manhattan(ManhattanArgs),

    /// Generate a batch of Manhattan plots from assets JSON
    ManhattanBatch(ManhattanBatchArgs),

    /// Generate locus plots from existing Manhattan output directory
    Loci(LociArgs),

    /// Render a LocusZoom-style scatter plot for a specific region
    Locus(LocusArgs),

    /// Manage a distributed worker pool for parallel processing
    Pool {
        #[command(subcommand)]
        command: PoolCommands,
    },

    /// Run distributed service components (coordinator or worker)
    Service {
        #[command(subcommand)]
        command: ServiceCommands,
    },
}

#[derive(Subcommand)]
pub enum ExportCommands {
    /// Convert to Parquet file
    Parquet(ExportParquetArgs),

    /// Export to JSON file (NDJSON)
    Json(ExportJsonArgs),

    /// Export to VCF file
    Vcf(ExportVcfArgs),

    /// Export to Hail Table format
    Hail(ExportHailArgs),

    /// Export to ClickHouse
    #[cfg(feature = "clickhouse")]
    Clickhouse(ExportClickhouseArgs),

    /// Export to BigQuery
    #[cfg(feature = "bigquery")]
    Bigquery(ExportBigqueryArgs),
}

/// Arguments for distributed processing (partition slicing).
/// Include these in any command that should support distributed execution.
#[derive(Args, Clone, Copy, Debug)]
pub struct PartitioningArgs {
    /// Worker ID (0-based) for distributed processing
    #[arg(long, default_value = "0")]
    pub worker_id: usize,

    /// Total number of workers in the pool
    #[arg(long, default_value = "1")]
    pub total_workers: usize,
}

impl PartitioningArgs {
    /// Returns true if this is a distributed job (more than one worker).
    pub fn is_distributed(&self) -> bool {
        self.total_workers > 1
    }
}

/// Common arguments shared by all export commands.
/// Use `#[command(flatten)]` to include these in export arg structs,
/// then implement `HasCommonExportArgs` for compile-time enforcement.
#[derive(Args)]
pub struct CommonExportArgs {
    /// Path to the Hail table
    pub input: String,

    /// Filter conditions (field=value, field>value, field>=value, etc.)
    #[arg(long = "where")]
    pub where_clauses: Vec<String>,

    /// Limit number of rows to export
    #[arg(long)]
    pub limit: Option<usize>,

    /// Genomic interval (chr:start-end format, can be specified multiple times)
    #[arg(long)]
    pub interval: Vec<String>,

    /// Path to interval file (.bed, .json, or text with chr:start-end lines)
    #[arg(long)]
    pub intervals_file: Option<String>,

    /// Partitioning arguments for distributed processing
    #[command(flatten)]
    pub partitioning: PartitioningArgs,

    /// Output progress as JSON lines (for distributed job coordination)
    #[arg(long, hide = true)]
    pub progress_json: bool,
}

/// Trait that all export argument structs must implement.
/// This enforces at compile time that all export targets have common args.
pub trait HasCommonExportArgs {
    fn common(&self) -> &CommonExportArgs;
}

#[derive(Args)]
pub struct ExportParquetArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output Parquet file path (or directory if --per-partition or --shard-count is used)
    pub output: String,

    /// Write each partition to a separate file in the output directory
    #[arg(long)]
    pub per_partition: bool,

    /// Write output as a directory of N Parquet files (groups partitions)
    #[arg(long, conflicts_with = "per_partition")]
    pub shard_count: Option<usize>,

    /// Collect and display system metrics during export (CPU, memory, I/O)
    #[arg(long)]
    pub benchmark: bool,
}

impl HasCommonExportArgs for ExportParquetArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[derive(Args)]
pub struct ExportJsonArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output JSON file path (or directory if --per-partition or --shard-count is used)
    pub output: String,

    /// Write each partition to a separate file in the output directory
    #[arg(long)]
    pub per_partition: bool,

    /// Write output as a directory of N JSON files (groups partitions)
    #[arg(long, conflicts_with = "per_partition")]
    pub shard_count: Option<usize>,

    /// Group rows by field value and write to separate files (not yet implemented)
    #[arg(long)]
    pub group_by: Option<String>,
}

impl HasCommonExportArgs for ExportJsonArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[derive(Args)]
pub struct ExportVcfArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output VCF file path
    pub output: String,

    /// Compress output with BGZF
    #[arg(long)]
    pub bgzip: bool,
}

impl HasCommonExportArgs for ExportVcfArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[derive(Args)]
pub struct ExportHailArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// Output Hail table directory path
    pub output: String,
}

impl HasCommonExportArgs for ExportHailArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[cfg(feature = "validation")]
#[derive(Subcommand)]
pub enum SchemaSubcommands {
    /// Validate table against JSON schema
    Validate(ValidateArgs),

    /// Generate JSON schema from table
    Generate(GenerateSchemaArgs),
}

#[cfg(feature = "validation")]
#[derive(Args)]
pub struct GenerateSchemaArgs {
    /// Path to the Hail table
    pub table: String,
    /// Output JSON schema file (stdout if not specified)
    pub output: Option<String>,
}

#[derive(Args)]
pub struct QueryArgs {
    /// Path to the Hail table or VCF file
    pub table: String,

    /// Point lookup (field=value)
    #[arg(long)]
    pub key: Option<String>,

    /// Filter conditions (field=value, field>value, field>=value, etc.)
    #[arg(long = "where")]
    pub where_clauses: Vec<String>,

    /// Limit number of results
    #[arg(long)]
    pub limit: Option<usize>,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,

    /// Genomic interval (chr:start-end format, can be specified multiple times)
    #[arg(long)]
    pub interval: Vec<String>,

    /// Path to interval file (.bed, .json, or text with chr:start-end lines)
    #[arg(long)]
    pub intervals_file: Option<String>,
}

#[cfg(feature = "clickhouse")]
#[derive(Args)]
pub struct ExportClickhouseArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// ClickHouse URL (e.g., http://localhost:8123)
    pub url: String,

    /// Target table name in ClickHouse
    pub table: String,
}

#[cfg(feature = "clickhouse")]
impl HasCommonExportArgs for ExportClickhouseArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[cfg(feature = "bigquery")]
#[derive(Args)]
pub struct ExportBigqueryArgs {
    #[command(flatten)]
    pub common: CommonExportArgs,

    /// BigQuery destination (project:dataset.table)
    pub destination: String,

    /// GCS bucket for staging parquet file
    #[arg(long)]
    pub bucket: String,

    /// Directory for temporary parquet file
    #[arg(long, default_value = "/tmp")]
    pub temp_dir: String,
}

#[cfg(feature = "bigquery")]
impl HasCommonExportArgs for ExportBigqueryArgs {
    fn common(&self) -> &CommonExportArgs {
        &self.common
    }
}

#[cfg(feature = "validation")]
#[derive(Args)]
pub struct ValidateArgs {
    /// Path to the Hail table
    pub table: String,

    /// Path to the JSON schema file
    pub schema: String,

    /// Validate first N rows (sequential)
    #[arg(long)]
    pub limit: Option<usize>,

    /// Validate N randomly sampled rows
    #[arg(long)]
    pub sample: Option<usize>,

    /// Stop on first validation error
    #[arg(long)]
    pub fail_fast: bool,

    /// Show each row ID and validation result in real-time (sequential)
    #[arg(long, short)]
    pub verbose: bool,
}

#[derive(Args, Debug)]
pub struct ManhattanArgs {
    // -- Data Inputs --
    /// Path to Exome results Hail Table
    #[arg(long)]
    pub exome: Option<String>,

    /// Path to Exome annotations Hail Table (for merge-join with exome results)
    #[arg(long)]
    pub exome_annotations: Option<String>,

    /// Path to Genome results Hail Table
    #[arg(long)]
    pub genome: Option<String>,

    /// Path to Genome annotations Hail Table (for merge-join with genome results)
    #[arg(long)]
    pub genome_annotations: Option<String>,

    /// Path to gene burden results Hail Table
    #[arg(long)]
    pub gene_burden: Option<String>,

    /// Path to gnomAD genes table (for gene bounds lookup and locus gene tracks)
    #[arg(long)]
    pub genes: Option<String>,

    // -- Legacy/Single Table Mode --
    /// Path to the variant results Hail table (legacy single-table mode)
    #[arg(long)]
    pub table: Option<String>,

    /// Path to annotation table for enriching significant hits (legacy mode)
    #[arg(long)]
    pub annotate: Option<String>,

    /// Fields to extract from annotation table (default: all value fields)
    #[arg(long, value_delimiter = ',')]
    pub annotate_fields: Vec<String>,

    // -- Thresholds & Config --
    /// Chromosomes to include (e.g. '1', '1,6,17', 'all' for genome-wide)
    #[arg(long, default_value = "all")]
    pub chrom: String,

    /// Field name for P-value (Y-axis, -log10 applied automatically)
    #[arg(long, default_value = "Pvalue")]
    pub y_field: String,

    /// P-value threshold for significant variants (default: 5e-8)
    #[arg(long, visible_alias = "variant-threshold", default_value = "5e-8")]
    pub threshold: f64,

    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[arg(long, default_value = "2.5e-6")]
    pub gene_threshold: f64,

    /// P-value threshold to buffer variants for locus plots (default: 0.01)
    #[arg(long, default_value = "0.01")]
    pub locus_threshold: f64,

    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[arg(long, default_value = "1000000")]
    pub locus_window: i32,

    /// Generate locus-zoom style plots for significant regions
    #[arg(long)]
    pub locus_plots: bool,

    // -- Distributed Aggregation --
    /// Path to directory containing distributed scan shards (part-*.json files).
    /// When specified, aggregates shards and renders final PNG instead of scanning tables.
    #[arg(long)]
    pub from_shards: Option<String>,

    // -- Output Options --
    /// Limit number of rows to process (for testing)
    #[arg(long)]
    pub limit: Option<usize>,

    /// Image width in pixels
    #[arg(long, default_value = "3000")]
    pub width: u32,

    /// Image height in pixels
    #[arg(long, default_value = "800")]
    pub height: u32,

    /// Output filename prefix or directory (produces {prefix}.png + {prefix}.json)
    #[arg(long)]
    pub output: Option<String>,

    /// Color scheme (classic = alternating gray/blue per chromosome)
    #[arg(long, default_value = "classic")]
    pub colors: String,

    // -- Distributed Processing --
    /// Partitioning arguments for distributed processing
    #[command(flatten)]
    pub partitioning: PartitioningArgs,

    /// Output progress as JSON lines (for distributed job coordination)
    #[arg(long, hide = true)]
    pub progress_json: bool,
}

/// Arguments for generating a batch of Manhattan plots from assets JSON.
///
/// This command reads an assets JSON file (from axaou-server query-assets) and
/// submits a batch of Manhattan plot jobs to the coordinator for parallel processing.
#[derive(Args, Debug)]
pub struct ManhattanBatchArgs {
    /// Path to assets JSON file (from axaou-server query-assets)
    #[arg(long)]
    pub assets_json: String,

    /// Base output directory (e.g., gs://bucket/manhattans)
    #[arg(long)]
    pub output_dir: String,

    /// Filter to specific analysis IDs (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub analysis_ids: Option<Vec<String>>,

    /// Filter to specific ancestry groups (comma-separated, e.g., "meta,eur,afr")
    #[arg(long, value_delimiter = ',')]
    pub ancestries: Option<Vec<String>>,

    /// Limit number of phenotypes to process
    #[arg(long)]
    pub limit: Option<usize>,

    // Common Manhattan Options (Global overrides)

    /// P-value threshold for significant variants (default: 5e-8)
    #[arg(long, default_value = "5e-8")]
    pub threshold: f64,

    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[arg(long, default_value = "2.5e-6")]
    pub gene_threshold: f64,

    /// P-value threshold to buffer variants for locus plots (default: 0.01)
    #[arg(long, default_value = "0.01")]
    pub locus_threshold: f64,

    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[arg(long, default_value = "1000000")]
    pub locus_window: i32,

    /// Generate locus-zoom style plots for significant regions
    #[arg(long)]
    pub locus_plots: bool,

    /// Image width in pixels
    #[arg(long, default_value = "3000")]
    pub width: u32,

    /// Image height in pixels
    #[arg(long, default_value = "800")]
    pub height: u32,

    /// Path to gnomAD genes table
    #[arg(long)]
    pub genes: Option<String>,

    /// Path to Exome annotations Hail Table
    #[arg(long)]
    pub exome_annotations: Option<String>,

    /// Path to Genome annotations Hail Table
    #[arg(long)]
    pub genome_annotations: Option<String>,

    // Distributed Processing

    /// Partitioning arguments for distributed processing
    #[command(flatten)]
    pub partitioning: PartitioningArgs,

    /// Output progress as JSON lines
    #[arg(long, hide = true)]
    pub progress_json: bool,
}

/// Arguments for generating locus plots from existing Manhattan output
#[derive(Args, Debug)]
pub struct LociArgs {
    /// Path to Manhattan output directory (contains *_significant.parquet files)
    #[arg(long)]
    pub dir: String,

    /// Path to Exome results Hail Table (for reading variants in locus regions)
    #[arg(long)]
    pub exome: Option<String>,

    /// Path to Genome results Hail Table (for reading variants in locus regions)
    #[arg(long)]
    pub genome: Option<String>,

    /// Path to gene burden results Hail Table (for seeding locus regions from significant genes)
    #[arg(long)]
    pub gene_burden: Option<String>,

    /// Window size (bp) around significant hits for locus plots (default: 1MB)
    #[arg(long, default_value = "1000000")]
    pub locus_window: i32,

    /// P-value threshold for significant variants (default: 5e-8)
    #[arg(long, default_value = "5e-8")]
    pub threshold: f64,

    /// Significance threshold for gene burden results (default: 2.5e-6)
    #[arg(long, default_value = "2.5e-6")]
    pub gene_threshold: f64,

    /// P-value field name in source tables
    #[arg(long, default_value = "Pvalue")]
    pub y_field: String,

    /// Number of parallel threads (default: 8)
    #[arg(long, default_value = "8")]
    pub threads: usize,
}

#[derive(Args, Debug)]
pub struct LocusArgs {
    /// Path to Exome results Hail Table
    #[arg(long)]
    pub exome: Option<String>,

    /// Path to Genome results Hail Table
    #[arg(long)]
    pub genome: Option<String>,

    /// Region to plot (format: chr:start-end)
    #[arg(long)]
    pub region: String,

    /// Output PNG path
    #[arg(long)]
    pub output: String,

    /// P-value field name
    #[arg(long, default_value = "Pvalue")]
    pub y_field: String,

    /// Significance threshold
    #[arg(long, default_value = "5e-8")]
    pub threshold: f64,

    /// Image width
    #[arg(long, default_value = "800")]
    pub width: u32,

    /// Image height
    #[arg(long, default_value = "400")]
    pub height: u32,

    /// Max Y-axis value (-log10 p)
    #[arg(long, default_value = "30.0")]
    pub y_max: f64,
}

/// Subcommands for managing distributed worker pools.
#[derive(Subcommand)]
pub enum PoolCommands {
    /// Create a new worker pool of GCP VMs
    ///
    /// If a pool profile with the same name exists in the config file,
    /// its settings will be used as defaults. CLI arguments override config.
    Create {
        /// Name of the pool (used for tagging and identification)
        /// If a profile with this name exists in config, its settings are used as defaults
        name: String,

        /// Number of worker VMs to create (default: 4, or from config profile)
        #[arg(long)]
        workers: Option<usize>,

        /// GCP machine type (default: c3-highcpu-22, or from config profile)
        #[arg(long)]
        machine_type: Option<String>,

        /// GCP zone for the VMs (default: us-central1-a, or from config)
        #[arg(long)]
        zone: Option<String>,

        /// Use spot/preemptible instances for cost savings
        #[arg(long)]
        spot: Option<bool>,

        /// GCP project ID (defaults to gcloud config or config file)
        #[arg(long)]
        project: Option<String>,

        /// VPC network name (defaults to "default" or config file)
        #[arg(long)]
        network: Option<String>,

        /// Subnet name (required if network is specified and not using default)
        #[arg(long)]
        subnet: Option<String>,

        /// Wait for VMs to be ready (startup script complete)
        #[arg(long)]
        wait: bool,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,

        /// Create a dedicated coordinator node for distributed processing
        #[arg(long)]
        with_coordinator: bool,
    },

    /// Submit a job to run on the worker pool
    Submit {
        /// Name of the pool to submit to
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// Path to the Linux-compiled binary (defaults to target/x86_64-unknown-linux-gnu/release/hail-decoder)
        #[arg(long)]
        binary: Option<String>,

        /// Automatically stop VMs after job completion to save costs
        #[arg(long)]
        auto_stop: bool,

        /// Force binary redeployment even if coordinator is already running
        #[arg(long)]
        redeploy_binary: bool,

        /// Automatically scale workers up for this job and down to 0 afterwards
        #[arg(long)]
        autoscale: bool,

        /// Force submission even if a job is already running (supersedes it)
        #[arg(long)]
        force: bool,

        /// Number of partitions per worker batch (higher = more parallelism per worker)
        #[arg(long)]
        batch_size: Option<usize>,

        /// The command to run on workers (everything after --)
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },

    /// Scale the number of workers in a pool
    Scale {
        /// Name of the pool
        name: String,

        /// Target number of workers
        #[arg(long)]
        workers: usize,

        /// GCP zone (defaults to us-central1-a)
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// Path to the Linux-compiled binary (optional)
        #[arg(long)]
        binary: Option<String>,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,
    },

    /// Destroy a worker pool and delete all VMs
    Destroy {
        /// Name of the pool to destroy
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// GCS bucket path to export metrics database before destruction (e.g., gs://my-bucket/metrics/)
        #[arg(long)]
        metrics_bucket: Option<String>,
    },

    /// List instances in a worker pool
    List {
        /// Name of the pool
        name: String,
    },

    /// Check status of a distributed job running on the pool
    Status {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,
    },

    /// Update the binary on a running pool (upload to coordinator, workers pull)
    UpdateBinary {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,

        /// Path to the Linux-compiled binary (defaults to target/x86_64-unknown-linux-gnu/release/hail-decoder)
        #[arg(long)]
        binary: Option<String>,

        /// Skip automatic Linux binary build (use existing binary)
        #[arg(long)]
        skip_build: bool,
    },

    /// Cancel a running job on the pool
    Cancel {
        /// Name of the pool
        name: String,

        /// GCP zone where the pool is located
        #[arg(long, default_value = "us-central1-a")]
        zone: String,
    },
}

/// Subcommands for running distributed service components.
#[derive(Subcommand)]
pub enum ServiceCommands {
    /// Start the coordinator server (manages work distribution)
    StartCoordinator {
        /// Port to listen on
        #[arg(long, default_value = "3000")]
        port: u16,

        /// Path to input Hail table (optional, can be set later via POST /api/job)
        #[arg(long)]
        input: Option<String>,

        /// Path to output directory (optional, can be set later via POST /api/job)
        #[arg(long)]
        output: Option<String>,

        /// Total number of partitions to process (optional, can be set later via POST /api/job)
        #[arg(long)]
        total_partitions: Option<usize>,

        /// Number of partitions to assign per work request
        #[arg(long, default_value = "10")]
        batch_size: usize,

        /// Timeout in seconds before rescheduling stale work
        #[arg(long, default_value = "600")]
        timeout: u64,
    },

    /// Start a worker process (connects to coordinator for work)
    StartWorker {
        /// Coordinator URL (e.g., http://10.0.0.5:3000)
        #[arg(long)]
        url: String,

        /// Unique worker ID
        #[arg(long)]
        worker_id: String,

        /// Poll interval in milliseconds when waiting for work
        #[arg(long, default_value = "2000")]
        poll_interval: u64,
    },
}

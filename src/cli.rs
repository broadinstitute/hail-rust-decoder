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
}

#[derive(Subcommand)]
pub enum ExportCommands {
    /// Convert to Parquet file
    Parquet(ExportParquetArgs),

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

    /// Output Parquet file path
    pub output: String,
}

impl HasCommonExportArgs for ExportParquetArgs {
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

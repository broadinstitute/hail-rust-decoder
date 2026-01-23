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
    /// Show basic table metadata
    Info {
        /// Path to the Hail table or VCF file
        path: String,
    },

    /// Show detailed table information including keys and index status
    Inspect {
        /// Path to the Hail table or VCF file
        path: String,
    },

    /// Show comprehensive table summary with statistics
    Summary {
        /// Path to the Hail table
        path: String,
    },

    /// Query the table with optional filters
    Query(QueryArgs),

    /// Convert to Parquet format
    Convert {
        /// Input Hail table path
        input: String,
        /// Output Parquet file path
        output: String,
    },

    /// Export to external databases
    #[cfg(any(feature = "clickhouse", feature = "bigquery"))]
    Export {
        #[command(subcommand)]
        command: ExportCommands,
    },

    /// Validate table against JSON schema
    #[cfg(feature = "validation")]
    Validate(ValidateArgs),

    /// Generate JSON schema from table
    #[cfg(feature = "validation")]
    GenerateSchema {
        /// Path to the Hail table
        table: String,
        /// Output JSON schema file (stdout if not specified)
        output: Option<String>,
    },
}

#[cfg(any(feature = "clickhouse", feature = "bigquery"))]
#[derive(Subcommand)]
pub enum ExportCommands {
    /// Export to ClickHouse
    #[cfg(feature = "clickhouse")]
    Clickhouse(ExportClickhouseArgs),

    /// Export to BigQuery
    #[cfg(feature = "bigquery")]
    Bigquery(ExportBigqueryArgs),
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
}

#[cfg(feature = "clickhouse")]
#[derive(Args)]
pub struct ExportClickhouseArgs {
    /// Path to the Hail table
    pub input: String,

    /// ClickHouse URL (e.g., http://localhost:8123)
    pub url: String,

    /// Target table name in ClickHouse
    pub table: String,

    /// Filter conditions (same as query command)
    #[arg(long = "where")]
    pub where_clauses: Vec<String>,

    /// Limit number of rows to export
    #[arg(long)]
    pub limit: Option<usize>,
}

#[cfg(feature = "bigquery")]
#[derive(Args)]
pub struct ExportBigqueryArgs {
    /// Path to the Hail table
    pub input: String,

    /// BigQuery destination (project:dataset.table)
    pub destination: String,

    /// GCS bucket for staging parquet file
    #[arg(long)]
    pub bucket: String,

    /// Filter conditions (same as query command)
    #[arg(long = "where")]
    pub where_clauses: Vec<String>,

    /// Limit number of rows to export
    #[arg(long)]
    pub limit: Option<usize>,

    /// Directory for temporary parquet file
    #[arg(long, default_value = "/tmp")]
    pub temp_dir: String,
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

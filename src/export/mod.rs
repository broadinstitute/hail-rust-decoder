//! Export functionality for Hail tables
//!
//! This module provides export capabilities to various external systems.
//!
//! # Modules
//! - `clickhouse`: Export to ClickHouse using Parquet as intermediate format (requires `clickhouse` feature)
//! - `bigquery`: Export to Google BigQuery using Parquet and GCS staging (requires `bigquery` feature)

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;

#[cfg(feature = "bigquery")]
pub use bigquery::{BigQueryClient, BigQueryError};
#[cfg(feature = "clickhouse")]
pub use clickhouse::{ClickHouseClient, ClickHouseError};

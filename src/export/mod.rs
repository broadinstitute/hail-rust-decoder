//! Export functionality for Hail tables
//!
//! This module provides export capabilities to various external systems.
//!
//! # Modules
//! - `clickhouse`: Export to ClickHouse using Parquet as an intermediate format
//! - `bigquery`: Export to Google BigQuery using Parquet and GCS staging (requires `bigquery` feature)

#[cfg(feature = "bigquery")]
pub mod bigquery;
pub mod clickhouse;

#[cfg(feature = "bigquery")]
pub use bigquery::{BigQueryClient, BigQueryError};
pub use clickhouse::{ClickHouseClient, ClickHouseError};

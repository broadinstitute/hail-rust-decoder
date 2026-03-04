//! Summary generation for Hail tables
//!
//! This module provides functionality to generate comprehensive summaries
//! of Hail tables, including:
//!
//! - Basic table metadata (size, partition count, etc.)
//! - Formatted schema display
//! - Field statistics from full table scan (min, max, null counts)
//!
//! # Example
//!
//! ```rust,no_run
//! use genohype_core::summary::{format_schema, StatsAccumulator};
//! use genohype_core::query::QueryEngine;
//!
//! // Format a schema for display
//! let formatted = format_schema("struct{locus:Locus(GRCh38),alleles:Array[str]}");
//! println!("{}", formatted);
//!
//! // Collect statistics during a scan
//! let mut stats = StatsAccumulator::new();
//! // ... process rows with stats.process_row(&row)
//! ```

pub mod schema;
pub mod stats;

pub use schema::{format_schema, format_schema_clean};
pub use stats::{FieldStat, StatsAccumulator};

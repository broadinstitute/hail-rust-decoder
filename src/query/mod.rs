//! Query capabilities for Hail tables
//!
//! This module provides:
//! - Partition pruning based on key ranges
//! - Key comparison and ordering
//! - Query types for point lookups and range scans
//! - High-level query engine for table operations
//! - Genomic interval lists for region filtering

mod engine;
mod intervals;
mod pruning;
mod stream;
mod types;

pub use engine::*;
pub use intervals::*;
pub use pruning::*;
pub use stream::*;
pub use types::*;

//! Hail index reading (B-tree based)
//!
//! This module implements B-tree index reading for Hail tables.
//! Indexes enable:
//! - Point lookups by key (O(log n))
//! - Range scans (efficient iteration over key ranges)
//! - Skip to specific offsets in partition files

mod metadata;
mod node;
mod reader;

pub use metadata::*;
pub use node::*;
pub use reader::*;

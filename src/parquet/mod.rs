//! Parquet conversion utilities
//!
//! This module provides functionality to convert Hail tables to Parquet format.
//!
//! # Modules
//! - `schema`: Convert Hail types to Arrow schema
//! - `builder`: Column builders for row-to-columnar conversion
//! - `writer`: High-level Parquet writer

pub mod builder;
pub mod schema;
pub mod writer;

pub use writer::{build_record_batch, ParquetWriter};

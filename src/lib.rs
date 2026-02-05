//! # Hail Decoder
//!
//! A pure Rust implementation for reading and decoding Hail table format.
//!
//! This library provides zero-dependency (except Rust ecosystem) tools for:
//! - Reading Hail table schemas from metadata.json.gz
//! - Streaming and decompressing Hail binary data blocks
//! - Decoding typed data using Hail's codec system
//! - Converting Hail tables to Apache Parquet format
//! - Streaming from cloud storage (GCS, S3)
//!
//! ## Architecture
//!
//! The decoder implements Hail's 4-layer buffer stack:
//! 1. Stream Block Buffer - reads block-framed data
//! 2. Zstd Decompression - decompresses each block
//! 3. Blocking Buffer - provides fixed-size buffering
//! 4. LEB128 Buffer - decodes variable-length integers
//!
//! ## Example
//!
//! ```rust,no_run
//! use hail_decoder::query::{QueryEngine, KeyRange, KeyValue};
//!
//! // Open a Hail table
//! let mut engine = QueryEngine::open("path/to/table.ht")?;
//!
//! // Query with key ranges
//! let ranges = vec![
//!     KeyRange::point("chrom".to_string(), KeyValue::String("10".to_string())),
//! ];
//!
//! let result = engine.query(&ranges)?;
//! println!("Found {} rows", result.rows.len());
//! # Ok::<(), hail_decoder::HailError>(())
//! ```

pub mod buffer;
pub mod cloud;
pub mod codec;
pub mod datasource;
pub mod distributed;
pub mod error;
pub mod export;
pub mod hail_adapter;
pub mod index;
pub mod io;
pub mod metadata;
pub mod parquet;
pub mod partitioning;
pub mod manhattan;
pub mod query;
pub mod schema;
pub mod summary;
pub mod vcf;

#[cfg(feature = "validation")]
pub mod validation;

pub mod benchmark;

pub use error::{HailError, Result};

/// Version of the Hail format this decoder supports
pub const SUPPORTED_FORMAT_VERSION: u32 = 67328;

/// Hail version this decoder was tested against
pub const TESTED_HAIL_VERSION: &str = "0.2.134-952ae203dbbe";

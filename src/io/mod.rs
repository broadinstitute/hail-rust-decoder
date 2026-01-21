//! Cloud storage IO abstraction for Hail tables
//!
//! This module provides a unified interface for reading data from both local
//! files and cloud storage (GCS, S3). The `CloudReader` struct implements
//! `std::io::Read` and `std::io::Seek`, allowing it to be used with the existing
//! synchronous buffer stack.

pub mod adapter;

pub use adapter::{get_reader, join_path, BoxedReader, CloudReader};

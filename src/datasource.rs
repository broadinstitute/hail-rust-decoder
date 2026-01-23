//! Data source abstraction for genomic tables
//!
//! This module defines the `DataSource` trait which provides a unified interface
//! for reading data from various formats (Hail Tables, VCFs, etc.).

use crate::codec::{EncodedType, EncodedValue};
use crate::query::KeyRange;
use crate::Result;

/// A source of genomic table data
///
/// This trait abstracts over different table formats (Hail Tables, VCFs, etc.)
/// and provides a common interface for querying and streaming data.
pub trait DataSource: Send + Sync {
    /// Get the row schema
    ///
    /// Returns the schema describing the structure of each row.
    fn row_type(&self) -> &EncodedType;

    /// Get global metadata (header information)
    ///
    /// For Hail tables, this is the globals struct.
    /// For VCFs, this is the header metadata.
    fn globals(&self) -> Result<EncodedValue>;

    /// Get the names of the key fields
    ///
    /// These are the fields used for indexing and partitioning.
    fn key_fields(&self) -> &[String];

    /// Get the total number of partitions
    ///
    /// For Hail tables, this is the number of partition files.
    /// For VCFs, this might be virtual partitions based on genomic regions.
    fn num_partitions(&self) -> usize;

    /// Scan a specific partition and return all matching rows
    ///
    /// This is used for parallel processing where each worker handles one partition.
    ///
    /// # Arguments
    /// * `partition_idx` - The partition index to scan
    /// * `ranges` - Key range constraints for filtering rows
    fn scan_partition(&self, partition_idx: usize, ranges: &[KeyRange]) -> Result<Vec<EncodedValue>>;

    /// Stream rows matching the given key ranges
    ///
    /// This is the primary method for querying. Implementations should return
    /// an iterator that yields rows lazily for memory-efficient processing.
    ///
    /// # Arguments
    /// * `ranges` - Key range constraints for filtering rows
    fn query_stream(&self, ranges: &[KeyRange]) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>>;

    /// Perform a point lookup for a specific key
    ///
    /// Returns the first row matching the key, or None if not found.
    /// This is an optimization for indexed sources that can do efficient
    /// random access lookups.
    ///
    /// Default implementation returns an error indicating lookup is not supported.
    /// Implementations with index support should override this.
    fn lookup(&self, _key: &EncodedValue) -> Result<Option<EncodedValue>> {
        Err(crate::HailError::Index(
            "Lookup not implemented for this data source".to_string(),
        ))
    }

    /// Sample random rows from the data source
    ///
    /// Returns a random sample of rows. Implementations may optimize this
    /// differently:
    /// - Hail tables: sample random partitions then random rows within
    /// - VCFs with tabix index: use index to seek to random positions
    /// - Unindexed sources: fall back to streaming with reservoir sampling
    ///
    /// Default implementation returns an error indicating sampling is not supported.
    /// Implementations should override this with optimized sampling strategies.
    fn sample_random(&self, _sample_size: usize) -> Result<Vec<EncodedValue>> {
        Err(crate::HailError::Index(
            "Random sampling not implemented for this data source".to_string(),
        ))
    }
}

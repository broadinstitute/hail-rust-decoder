//! Query engine for Hail tables
//!
//! Provides a high-level API for querying Hail tables and other genomic data sources.
//! Delegates actual data access to implementations of the `DataSource` trait.

use crate::codec::{EncodedType, EncodedValue};
use crate::datasource::DataSource;
use crate::hail_adapter::HailTableSource;
use crate::metadata::RVDComponentSpec;
use crate::query::KeyRange;
use crate::Result;
use std::path::Path;

/// High-level query engine for Hail tables and other data sources
///
/// Supports both local and cloud storage paths:
/// - Local: `path/to/table.ht`
/// - GCS: `gs://bucket/path/to/table.ht`
/// - S3: `s3://bucket/path/to/table.ht`
pub struct QueryEngine {
    /// The underlying data source
    source: Box<dyn DataSource>,
    /// Keep raw hail source for inspection if needed (Hail tables only)
    hail_source: Option<HailTableSource>,
}

/// Result of a query operation
#[derive(Debug)]
pub struct QueryResult {
    /// The decoded rows matching the query
    pub rows: Vec<EncodedValue>,
    /// Number of partitions scanned
    pub partitions_scanned: usize,
    /// Number of partitions pruned (skipped)
    pub partitions_pruned: usize,
}

impl QueryEngine {
    /// Open a Hail table for querying from a local path
    ///
    /// # Arguments
    /// * `table_path` - Path to the table directory (e.g., "path/to/table.ht")
    ///
    /// # Example
    /// ```no_run
    /// use hail_decoder::query::QueryEngine;
    ///
    /// let engine = QueryEngine::open("data/my_table.ht").unwrap();
    /// ```
    pub fn open<P: AsRef<Path>>(table_path: P) -> Result<Self> {
        let path_str = table_path.as_ref().to_string_lossy().to_string();
        Self::open_path(&path_str)
    }

    /// Open a Hail table for querying from a path string (local or cloud URL)
    ///
    /// # Arguments
    /// * `table_path` - Path to the table directory
    ///   - Local: `path/to/table.ht`
    ///   - GCS: `gs://bucket/path/to/table.ht`
    ///   - S3: `s3://bucket/path/to/table.ht`
    ///
    /// # Example
    /// ```no_run
    /// use hail_decoder::query::QueryEngine;
    ///
    /// // Local file
    /// let engine = QueryEngine::open_path("data/my_table.ht").unwrap();
    ///
    /// // Google Cloud Storage
    /// let engine = QueryEngine::open_path("gs://my-bucket/data/my_table.ht").unwrap();
    /// ```
    pub fn open_path(table_path: &str) -> Result<Self> {
        // For now, assume everything is a Hail Table
        // In Phase 4, we will add VCF support here
        let hail_source = HailTableSource::new(table_path)?;

        // Create a second instance for the trait object
        // (This is necessary because we need to keep Hail-specific access for rvd_spec())
        let source = Box::new(HailTableSource::new(table_path)?);

        Ok(QueryEngine {
            source,
            hail_source: Some(hail_source),
        })
    }

    /// Get the key field names for this table
    pub fn key_fields(&self) -> &[String] {
        self.source.key_fields()
    }

    /// Get the number of partitions in this table
    pub fn num_partitions(&self) -> usize {
        self.source.num_partitions()
    }

    /// Check if this table has indexes
    pub fn has_index(&self) -> bool {
        // Only Hail tables have indexes in the current implementation
        if let Some(hail) = &self.hail_source {
            hail.rvd_spec().index_spec.is_some()
        } else {
            false
        }
    }

    /// Get the RVD specification (Hail tables only)
    ///
    /// Returns reference to RVD spec or panics if not a Hail table.
    /// This is used by inspection commands.
    pub fn rvd_spec(&self) -> &RVDComponentSpec {
        if let Some(hail) = &self.hail_source {
            hail.rvd_spec()
        } else {
            panic!("Not a Hail table")
        }
    }

    /// Get the row type (schema) for this table
    pub fn row_type(&self) -> &EncodedType {
        self.source.row_type()
    }

    /// Perform a point lookup by key
    ///
    /// Returns the row matching the exact key, or None if not found.
    ///
    /// # Arguments
    /// * `key` - The key value to search for (must match the table's key structure)
    ///
    /// # Example
    /// ```no_run
    /// use hail_decoder::query::{QueryEngine, KeyValue};
    /// use hail_decoder::codec::EncodedValue;
    ///
    /// let mut engine = QueryEngine::open("data/my_table.ht").unwrap();
    ///
    /// // Create key for lookup
    /// let key = EncodedValue::Struct(vec![
    ///     ("gene_id".to_string(), EncodedValue::Binary(b"ENSG00000141510".to_vec())),
    /// ]);
    ///
    /// if let Some(row) = engine.lookup(&key).unwrap() {
    ///     println!("Found: {:?}", row);
    /// }
    /// ```
    pub fn lookup(&mut self, key: &EncodedValue) -> Result<Option<EncodedValue>> {
        self.source.lookup(key)
    }

    /// Query with key ranges (for range scans)
    ///
    /// Returns all rows within the specified key range.
    ///
    /// # Arguments
    /// * `ranges` - Key range constraints for the query
    ///
    /// # Example
    /// ```no_run
    /// use hail_decoder::query::{QueryEngine, KeyRange, KeyValue};
    ///
    /// let mut engine = QueryEngine::open("data/my_table.ht").unwrap();
    ///
    /// // Query for all rows where chrom = "chr1"
    /// let ranges = vec![
    ///     KeyRange::point("chrom".to_string(), KeyValue::String("chr1".to_string())),
    /// ];
    ///
    /// let result = engine.query(&ranges).unwrap();
    /// println!("Found {} rows", result.rows.len());
    /// ```
    pub fn query(&mut self, ranges: &[KeyRange]) -> Result<QueryResult> {
        let total_partitions = self.source.num_partitions();

        // Collect all rows
        let iter = self.source.query_stream(ranges)?;
        let rows: Result<Vec<EncodedValue>> = iter.collect();
        let rows = rows?;

        Ok(QueryResult {
            rows,
            partitions_scanned: total_partitions,
            partitions_pruned: 0,
        })
    }

    /// Create a streaming iterator for a single partition
    ///
    /// # Arguments
    /// * `partition_idx` - The index of the partition to scan
    /// * `ranges` - Key range constraints for filtering rows
    ///
    /// # Returns
    /// A vector of decoded rows (for compatibility with existing API)
    pub fn scan_partition(&self, partition_idx: usize, ranges: &[KeyRange]) -> Result<Vec<EncodedValue>> {
        self.source.scan_partition(partition_idx, ranges)
    }

    /// Query with streaming output
    ///
    /// Returns an iterator that yields rows as they are read, instead of
    /// collecting all results into memory first. This is more memory-efficient
    /// for large result sets and allows processing to start immediately.
    ///
    /// # Arguments
    /// * `ranges` - Key range constraints for the query
    ///
    /// # Example
    /// ```no_run
    /// use hail_decoder::query::{QueryEngine, KeyRange, KeyValue};
    ///
    /// let engine = QueryEngine::open("data/my_table.ht").unwrap();
    ///
    /// // Stream results with a limit
    /// for row in engine.query_iter(&[]).unwrap().take(100) {
    ///     let row = row.unwrap();
    ///     println!("{:?}", row);
    /// }
    /// ```
    pub fn query_iter(
        &self,
        ranges: &[KeyRange],
    ) -> Result<impl Iterator<Item = Result<EncodedValue>>> {
        self.source.query_stream(ranges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_engine_open() {
        let engine = QueryEngine::open("data/gene_models_hds/ht/prep_table.ht")
            .expect("Failed to open table");

        assert_eq!(engine.key_fields(), &["gene_id", "chrom", "start"]);
        assert_eq!(engine.num_partitions(), 1);
        assert!(engine.has_index());
    }

    #[test]
    fn test_query_engine_lookup() {
        let mut engine = QueryEngine::open("data/gene_models_hds/ht/prep_table.ht")
            .expect("Failed to open table");

        // Create a key to lookup
        let key = EncodedValue::Struct(vec![
            ("gene_id".to_string(), EncodedValue::Binary(b"ENSG00000066468".to_vec())),
            ("chrom".to_string(), EncodedValue::Binary(b"10".to_vec())),
            ("start".to_string(), EncodedValue::Int32(121478332)),
        ]);

        let result = engine.lookup(&key);
        assert!(result.is_ok(), "Lookup failed: {:?}", result.err());
    }
}

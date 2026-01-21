//! Query engine for Hail tables
//!
//! Provides a high-level API for querying Hail tables using:
//! - Partition pruning based on key ranges
//! - B-tree index lookups for efficient point queries
//! - Row decoding from partition files
//!
//! Supports both local and cloud storage paths (GCS, S3).

use crate::buffer::{BlockMap, BufferBuilder, InputBuffer, LEB128Buffer, SliceBuffer};
use crate::codec::{EncodedType, EncodedValue, ETypeParser};
use crate::index::IndexReader;
use crate::io::{join_path, read_single_block};
use crate::metadata::{IndexSpec, RVDComponentSpec};
use crate::query::{filter_partitions, KeyRange, KeyValue};
use crate::HailError;
use crate::Result;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::Path;

/// High-level query engine for Hail tables
///
/// Supports both local and cloud storage paths:
/// - Local: `path/to/table.ht`
/// - GCS: `gs://bucket/path/to/table.ht`
/// - S3: `s3://bucket/path/to/table.ht`
pub struct QueryEngine {
    /// Base path to the table (e.g., "path/to/table.ht" or "gs://bucket/table.ht")
    table_path: String,
    /// Path to the rows directory
    rows_path: String,
    /// RVD metadata for rows
    rvd_spec: RVDComponentSpec,
    /// Parsed row type for decoding
    row_type: EncodedType,
    /// Cached index readers (one per partition)
    index_readers: HashMap<usize, IndexReader>,
    /// Cached block maps for efficient random access (one per partition)
    block_maps: HashMap<usize, BlockMap>,
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
        let table_path = table_path.trim_end_matches('/').to_string();
        let rows_path = join_path(&table_path, "rows");

        // Load RVD metadata
        let metadata_path = join_path(&rows_path, "metadata.json.gz");
        let rvd_spec = RVDComponentSpec::from_path(&metadata_path)?;

        // Parse the row type from the codec spec
        let row_type = ETypeParser::parse(&rvd_spec.codec_spec.e_type)?;

        Ok(QueryEngine {
            table_path,
            rows_path,
            rvd_spec,
            row_type,
            index_readers: HashMap::new(),
            block_maps: HashMap::new(),
        })
    }

    /// Get the key field names for this table
    pub fn key_fields(&self) -> &[String] {
        &self.rvd_spec.key
    }

    /// Get the number of partitions in this table
    pub fn num_partitions(&self) -> usize {
        self.rvd_spec.part_files.len()
    }

    /// Check if this table has indexes
    pub fn has_index(&self) -> bool {
        self.rvd_spec.index_spec.is_some()
    }

    /// Get the RVD specification
    pub fn rvd_spec(&self) -> &RVDComponentSpec {
        &self.rvd_spec
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
        // For point lookups, skip partition pruning if there's only one partition
        // (partition pruning with composite keys is complex and the current
        // implementation has limitations with multi-field keys)
        let matching_partitions = if self.rvd_spec.part_files.len() == 1 {
            vec![0] // Use the single partition
        } else {
            // Convert key to KeyRanges for partition pruning
            let key_ranges = self.key_to_ranges(key)?;
            filter_partitions(&self.rvd_spec.range_bounds, &key_ranges)
        };

        if matching_partitions.is_empty() {
            return Ok(None);
        }

        // For point lookups, we expect at most one partition to match
        // (assuming proper key ordering across partitions)
        for partition_idx in matching_partitions {
            // Get or create index reader for this partition
            let offset = self.lookup_in_index(partition_idx, key)?;

            if let Some(data_offset) = offset {
                // Read the row from the partition file at the given offset
                let row = self.read_row_at_offset(partition_idx, data_offset)?;
                return Ok(Some(row));
            }
        }

        Ok(None)
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
        let total_partitions = self.num_partitions();

        // Prune partitions based on ranges
        let matching_partitions = filter_partitions(&self.rvd_spec.range_bounds, ranges);
        let partitions_pruned = total_partitions - matching_partitions.len();

        // Scan partitions in parallel using rayon
        let results: Result<Vec<Vec<EncodedValue>>> = matching_partitions
            .par_iter()
            .map(|&partition_idx| self.scan_partition(partition_idx, ranges))
            .collect();

        // Flatten results from all partitions
        let rows: Vec<EncodedValue> = results?.into_iter().flatten().collect();

        Ok(QueryResult {
            rows,
            partitions_scanned: matching_partitions.len(),
            partitions_pruned,
        })
    }

    /// Convert an EncodedValue key to KeyRanges for partition pruning
    fn key_to_ranges(&self, key: &EncodedValue) -> Result<Vec<KeyRange>> {
        let mut ranges = Vec::new();

        if let EncodedValue::Struct(fields) = key {
            for (field_name, field_value) in fields {
                if let Some(key_value) = self.encoded_to_key_value(field_value) {
                    ranges.push(KeyRange::point(field_name.clone(), key_value));
                }
            }
        }

        Ok(ranges)
    }

    /// Convert an EncodedValue to a KeyValue
    fn encoded_to_key_value(&self, value: &EncodedValue) -> Option<KeyValue> {
        match value {
            EncodedValue::Binary(b) => {
                Some(KeyValue::String(String::from_utf8_lossy(b).into_owned()))
            }
            EncodedValue::Int32(i) => Some(KeyValue::Int32(*i)),
            EncodedValue::Int64(i) => Some(KeyValue::Int64(*i)),
            EncodedValue::Float32(f) => Some(KeyValue::Float32(*f)),
            EncodedValue::Float64(f) => Some(KeyValue::Float64(*f)),
            EncodedValue::Boolean(b) => Some(KeyValue::Boolean(*b)),
            _ => None,
        }
    }

    /// Lookup a key in the index for a specific partition
    fn lookup_in_index(&mut self, partition_idx: usize, key: &EncodedValue) -> Result<Option<i64>> {
        let index_spec = self.rvd_spec.index_spec.as_ref().ok_or_else(|| {
            HailError::Index("Table does not have an index".to_string())
        })?;

        // Get or create index reader
        if !self.index_readers.contains_key(&partition_idx) {
            let reader = self.create_index_reader(partition_idx, index_spec)?;
            self.index_readers.insert(partition_idx, reader);
        }

        let reader = self.index_readers.get(&partition_idx).unwrap();
        reader.lookup(key)
    }

    /// Create an index reader for a partition
    fn create_index_reader(&self, partition_idx: usize, index_spec: &IndexSpec) -> Result<IndexReader> {
        // Get the partition file name and derive the index directory name
        let part_file = &self.rvd_spec.part_files[partition_idx];

        // Index directory is named like the partition file but with .idx extension
        // e.g., part-0-xxx -> part-0-xxx.idx
        let index_dir_name = format!("{}.idx", part_file);
        let index_rel_path = index_spec.rel_path.trim_start_matches("../");
        let index_base = join_path(&self.table_path, index_rel_path);
        let index_path = join_path(&index_base, &index_dir_name);

        IndexReader::new_from_path(&index_path, index_spec)
    }

    /// Get the path to a partition file
    fn get_partition_path(&self, partition_idx: usize) -> String {
        let part_file = &self.rvd_spec.part_files[partition_idx];
        let parts_path = join_path(&self.rows_path, "parts");
        join_path(&parts_path, part_file)
    }

    /// Get or create a block map for a partition
    fn get_or_create_block_map(&mut self, partition_idx: usize) -> Result<&BlockMap> {
        if !self.block_maps.contains_key(&partition_idx) {
            let part_path = self.get_partition_path(partition_idx);
            let block_map = BlockMap::build_from_path(&part_path)?;
            self.block_maps.insert(partition_idx, block_map);
        }
        Ok(self.block_maps.get(&partition_idx).unwrap())
    }

    /// Read a row from a partition file at a specific offset using the block map
    ///
    /// This is the efficient implementation that:
    /// 1. Uses the block map to find which compressed block contains the offset
    /// 2. Reads only that single block using a range request (for cloud storage)
    /// 3. Decompresses just the one block
    /// 4. Reads the row from the correct position within the decompressed block
    fn read_row_at_offset(&mut self, partition_idx: usize, offset: i64) -> Result<EncodedValue> {
        let part_path = self.get_partition_path(partition_idx);

        // Get or create the block map for this partition
        let block_map = self.get_or_create_block_map(partition_idx)?;

        // Find the block containing our offset
        let block_entry = block_map.find_block(offset as u64).ok_or_else(|| {
            HailError::Index(format!(
                "Offset {} is out of bounds for partition {}",
                offset, partition_idx
            ))
        })?;

        // Calculate the offset within the decompressed block
        let local_offset = (offset as u64 - block_entry.decompressed_offset) as usize;

        // Read just this one block from the file
        let block_data = read_single_block(&part_path, block_entry.file_offset)?;

        // The block data format is: [4-byte decompressed_size][zstd_compressed_data]
        if block_data.len() < 4 {
            return Err(HailError::InvalidFormat(
                "Block too small for decompressed size header".to_string(),
            ));
        }

        // Decompress the block
        let decompressed = zstd::decode_all(&block_data[4..]).map_err(|_| HailError::Zstd)?;

        // Create a buffer for reading from the decompressed data at the local offset
        // We need LEB128Buffer on top of SliceBuffer because Hail uses LEB128 encoding
        let slice_buffer = SliceBuffer::new(&decompressed[local_offset..]);
        let mut buffer = LEB128Buffer::new(slice_buffer);

        // Read and decode the row
        // NOTE: Hail partition files ALWAYS have a row present flag byte before each row,
        // regardless of whether the row type is marked as required.
        let row_present = buffer.read_bool()?;
        if !row_present {
            return Ok(EncodedValue::Null);
        }
        self.row_type.read_present_value(&mut buffer)
    }

    /// Scan a partition and filter rows by key ranges
    ///
    /// # Arguments
    /// * `partition_idx` - The index of the partition to scan
    /// * `ranges` - Key range constraints for filtering rows
    ///
    /// # Returns
    /// A vector of decoded rows that match the filter criteria
    pub fn scan_partition(&self, partition_idx: usize, ranges: &[KeyRange]) -> Result<Vec<EncodedValue>> {
        let part_file = &self.rvd_spec.part_files[partition_idx];
        let parts_path = join_path(&self.rows_path, "parts");
        let part_path = join_path(&parts_path, part_file);

        // Build the buffer stack (works with both local and cloud paths)
        let mut buffer = BufferBuilder::from_path(&part_path)?
            .with_leb128()
            .build();

        let mut rows = Vec::new();

        // Read all rows from the partition
        // NOTE: Hail partition files ALWAYS have a row present flag byte before each row,
        // regardless of whether the row type is marked as required.
        loop {
            // Read row present flag first
            let row_present = match buffer.read_bool() {
                Ok(present) => present,
                Err(HailError::UnexpectedEof) => break,
                Err(e) => return Err(e),
            };

            if !row_present {
                // Skip null rows
                continue;
            }

            // Decode the row data
            match self.row_type.read_present_value(&mut buffer) {
                Ok(row) => {
                    // Check if row matches the range filters
                    if self.row_matches_ranges(&row, ranges) {
                        rows.push(row);
                    }
                }
                Err(HailError::UnexpectedEof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(rows)
    }

    /// Check if a row matches all the key range filters
    fn row_matches_ranges(&self, row: &EncodedValue, ranges: &[KeyRange]) -> bool {
        for range in ranges {
            // Extract the value using the field path (supports nested access)
            let field_value = self.extract_field_by_path(row, &range.field_path);

            if let Some(value) = field_value {
                if let Some(key_value) = self.encoded_to_key_value(value) {
                    if !self.value_in_range(&key_value, range) {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Extract a field from an EncodedValue by following a field path
    fn extract_field_by_path<'a>(
        &self,
        value: &'a EncodedValue,
        path: &[String],
    ) -> Option<&'a EncodedValue> {
        if path.is_empty() {
            return Some(value);
        }

        let mut current = value;
        for field_name in path {
            match current {
                EncodedValue::Struct(fields) => {
                    // Find the field by name
                    current = fields
                        .iter()
                        .find(|(name, _)| name == field_name)
                        .map(|(_, v)| v)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    /// Check if a value is within a key range
    fn value_in_range(&self, value: &KeyValue, range: &KeyRange) -> bool {
        use crate::query::QueryBound;

        // Check start bound
        match &range.start {
            QueryBound::Included(start) => {
                if value < start {
                    return false;
                }
            }
            QueryBound::Excluded(start) => {
                if value <= start {
                    return false;
                }
            }
            QueryBound::Unbounded => {}
        }

        // Check end bound
        match &range.end {
            QueryBound::Included(end) => {
                if value > end {
                    return false;
                }
            }
            QueryBound::Excluded(end) => {
                if value >= end {
                    return false;
                }
            }
            QueryBound::Unbounded => {}
        }

        true
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

        // The lookup should find the row (offset 0 from index)
        // Note: The actual row decoding may need refinement
    }
}

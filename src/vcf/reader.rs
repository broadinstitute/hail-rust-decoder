//! VCF Data Source
//!
//! Implements the `DataSource` trait for VCF files using noodles.
//! Supports both plain VCF and BGZF-compressed VCF files (.vcf.gz).
//! Optional Tabix index support for efficient region queries.

use crate::codec::{EncodedType, EncodedValue};
use crate::datasource::DataSource;
use crate::query::{KeyRange, KeyValue, QueryBound};
use crate::{HailError, Result};
use noodles::bgzf;
use noodles::core::Region;
use noodles::tabix;
use noodles::vcf;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use tracing::{debug, info};

/// DataSource implementation for VCF files
///
/// Provides read access to VCF files using noodles library.
/// Supports:
/// - Plain VCF files
/// - BGZF-compressed VCF files (.vcf.gz)
/// - Tabix-indexed VCF files (.vcf.gz.tbi) for efficient region queries
pub struct VcfDataSource {
    /// Path to the VCF file
    path: String,
    /// Parsed VCF header
    header: Arc<vcf::Header>,
    /// Generated schema from header
    schema: EncodedType,
    /// Optional Tabix index for region queries
    index: Option<tabix::Index>,
    /// Whether the file is BGZF compressed
    is_bgzf: bool,
}

impl VcfDataSource {
    /// Open a VCF file for reading
    ///
    /// # Arguments
    /// * `path` - Path to the VCF file (local or cloud URL)
    ///
    /// Automatically detects:
    /// - BGZF compression (by .gz extension)
    /// - Tabix index presence (.tbi file)
    pub fn new(path: &str) -> Result<Self> {
        let is_bgzf = path.ends_with(".gz") || path.ends_with(".bgz");

        // Read header
        let header = if is_bgzf {
            let reader = crate::io::get_reader(path)?;
            let bgzf_reader = bgzf::Reader::new(reader);
            let mut vcf_reader = vcf::io::Reader::new(BufReader::new(bgzf_reader));
            vcf_reader.read_header().map_err(HailError::Io)?
        } else {
            let reader = crate::io::get_reader(path)?;
            let mut vcf_reader = vcf::io::Reader::new(BufReader::new(reader));
            vcf_reader.read_header().map_err(HailError::Io)?
        };

        // Generate schema from header
        let schema = super::schema::extract_schema_from_header(&header)?;

        // Try to load tabix index
        let index = Self::load_index(path);
        if index.is_some() {
            debug!("Loaded tabix index for {}", path);
        }

        Ok(Self {
            path: path.to_string(),
            header: Arc::new(header),
            schema,
            index,
            is_bgzf,
        })
    }

    /// Try to load a tabix index for the VCF file
    fn load_index(vcf_path: &str) -> Option<tabix::Index> {
        let index_path = format!("{}.tbi", vcf_path);
        match crate::io::get_reader(&index_path) {
            Ok(reader) => {
                let mut tabix_reader = tabix::io::Reader::new(reader);
                tabix_reader.read_index().ok()
            }
            Err(_) => None,
        }
    }

    /// Convert KeyRanges to a noodles Region for indexed queries
    ///
    /// Currently supports:
    /// - Point queries on locus.contig (chromosome)
    /// - Range queries on locus.position
    fn ranges_to_region(&self, ranges: &[KeyRange]) -> Option<Region> {
        let mut contig: Option<String> = None;
        let mut start: Option<usize> = None;
        let mut end: Option<usize> = None;

        for range in ranges {
            // Check for locus.contig
            if range.field_path.len() >= 2
                && range.field_path[0] == "locus"
                && range.field_path[1] == "contig"
            {
                if let (
                    QueryBound::Included(KeyValue::String(s)),
                    QueryBound::Included(KeyValue::String(e)),
                ) = (&range.start, &range.end)
                {
                    if s == e {
                        contig = Some(s.clone());
                    }
                }
            }

            // Check for locus.position
            if range.field_path.len() >= 2
                && range.field_path[0] == "locus"
                && range.field_path[1] == "position"
            {
                // Extract start position
                match &range.start {
                    QueryBound::Included(KeyValue::Int32(v)) => start = Some(*v as usize),
                    QueryBound::Excluded(KeyValue::Int32(v)) => start = Some((*v + 1) as usize),
                    QueryBound::Unbounded => {}
                    _ => {}
                }
                // Extract end position
                match &range.end {
                    QueryBound::Included(KeyValue::Int32(v)) => end = Some(*v as usize),
                    QueryBound::Excluded(KeyValue::Int32(v)) => end = Some((*v - 1) as usize),
                    QueryBound::Unbounded => {}
                    _ => {}
                }
            }
        }

        // Build region if we have at least a contig
        contig.map(|c| {
            use noodles::core::Position;
            match (start, end) {
                (Some(s), Some(e)) => {
                    let start_pos = Position::try_from(s).unwrap_or(Position::MIN);
                    let end_pos = Position::try_from(e).unwrap_or(Position::MAX);
                    Region::new(c, start_pos..=end_pos)
                }
                (Some(s), None) => {
                    let start_pos = Position::try_from(s).unwrap_or(Position::MIN);
                    Region::new(c, start_pos..)
                }
                (None, Some(e)) => {
                    let end_pos = Position::try_from(e).unwrap_or(Position::MAX);
                    Region::new(c, ..=end_pos)
                }
                (None, None) => {
                    // Just contig, no position constraints
                    c.parse().unwrap_or_else(|_| Region::new(c, ..))
                }
            }
        })
    }

    /// Create an iterator for a full file scan
    fn full_scan_iter(&self) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        let header = self.header.clone();

        if self.is_bgzf {
            let reader = crate::io::get_reader(&self.path)?;
            let bgzf_reader = bgzf::Reader::new(reader);
            let mut vcf_reader = vcf::io::Reader::new(BufReader::new(bgzf_reader));

            // Skip header (we already have it)
            let _ = vcf_reader.read_header().map_err(HailError::Io)?;

            let iter = VcfRecordIterator {
                reader: Box::new(vcf_reader),
                header,
            };
            Ok(Box::new(iter))
        } else {
            let reader = crate::io::get_reader(&self.path)?;
            let mut vcf_reader = vcf::io::Reader::new(BufReader::new(reader));

            // Skip header
            let _ = vcf_reader.read_header().map_err(HailError::Io)?;

            let iter = VcfRecordIterator {
                reader: Box::new(vcf_reader),
                header,
            };
            Ok(Box::new(iter))
        }
    }

    /// Perform an indexed query on a local file
    fn indexed_query_local(
        &self,
        region: &Region,
        index: &tabix::Index,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Open indexed reader using the file path
        let mut indexed_reader = vcf::io::indexed_reader::Builder::default()
            .set_index(index.clone())
            .build_from_path(&self.path)
            .map_err(HailError::Io)?;

        // Read header
        let header = Arc::new(indexed_reader.read_header().map_err(HailError::Io)?);

        // Create query iterator
        let query = indexed_reader
            .query(&header, region)
            .map_err(HailError::Io)?;

        // Note: The noodles Query iterator borrows from the reader, so we can't
        // easily return it as a streaming iterator. We collect records eagerly.
        // For huge result sets, consider a different approach (channel-based).
        let header_clone = header.clone();
        let ranges = ranges.to_vec();
        let records: Vec<Result<EncodedValue>> = query
            .map(|result| {
                result
                    .map_err(HailError::Io)
                    .and_then(|record| super::codec::record_to_row_lazy(&header_clone, &record))
            })
            .filter(|result| match result {
                Ok(row) => ranges.is_empty() || row_matches_ranges(row, &ranges),
                Err(_) => true,
            })
            .collect();

        info!("Indexed query returned {} records", records.len());
        Ok(Box::new(records.into_iter()))
    }

    /// Perform an indexed query on a remote file (GCS, S3, HTTP)
    fn indexed_query_remote(
        &self,
        region: &Region,
        index: &tabix::Index,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Get a reader for the remote file
        let reader = crate::io::get_reader(&self.path)?;

        // Create indexed reader using build_from_reader
        // This wraps our BoxedReader in bgzf::io::Reader
        let mut indexed_reader = vcf::io::indexed_reader::Builder::default()
            .set_index(index.clone())
            .build_from_reader(reader)
            .map_err(HailError::Io)?;

        // Read header
        let header = Arc::new(indexed_reader.read_header().map_err(HailError::Io)?);

        // Create query iterator
        let query = indexed_reader
            .query(&header, region)
            .map_err(HailError::Io)?;

        // Collect records eagerly (same as local)
        let header_clone = header.clone();
        let ranges = ranges.to_vec();
        let records: Vec<Result<EncodedValue>> = query
            .map(|result| {
                result
                    .map_err(HailError::Io)
                    .and_then(|record| super::codec::record_to_row_lazy(&header_clone, &record))
            })
            .filter(|result| match result {
                Ok(row) => ranges.is_empty() || row_matches_ranges(row, &ranges),
                Err(_) => true,
            })
            .collect();

        info!("Remote indexed query returned {} records", records.len());
        Ok(Box::new(records.into_iter()))
    }
}

/// Iterator over VCF records that converts them to EncodedValue
struct VcfRecordIterator<R: BufRead> {
    reader: Box<vcf::io::Reader<R>>,
    header: Arc<vcf::Header>,
}

impl<R: BufRead + Send + 'static> Iterator for VcfRecordIterator<R> {
    type Item = Result<EncodedValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = vcf::variant::RecordBuf::default();
        match self.reader.read_record_buf(&self.header, &mut record) {
            Ok(0) => None, // EOF
            Ok(_) => Some(super::codec::record_to_row(&self.header, &record)),
            Err(e) => Some(Err(HailError::Io(e))),
        }
    }
}

// We need Send for the iterator
unsafe impl<R: BufRead + Send> Send for VcfRecordIterator<R> {}

/// Check if a row matches all the given key ranges
fn row_matches_ranges(row: &EncodedValue, ranges: &[KeyRange]) -> bool {
    for range in ranges {
        if !row_matches_single_range(row, range) {
            return false;
        }
    }
    true
}

/// Check if a row matches a single key range
fn row_matches_single_range(row: &EncodedValue, range: &KeyRange) -> bool {
    // Extract the field value using the field path
    let field_value = get_nested_field(row, &range.field_path);
    let field_value = match field_value {
        Some(v) => v,
        None => return false, // Missing field doesn't match
    };

    // Compare against range bounds
    let cmp_start = match &range.start {
        QueryBound::Unbounded => true,
        QueryBound::Included(key) => compare_values(field_value, key) >= std::cmp::Ordering::Equal,
        QueryBound::Excluded(key) => compare_values(field_value, key) > std::cmp::Ordering::Equal,
    };

    let cmp_end = match &range.end {
        QueryBound::Unbounded => true,
        QueryBound::Included(key) => compare_values(field_value, key) <= std::cmp::Ordering::Equal,
        QueryBound::Excluded(key) => compare_values(field_value, key) < std::cmp::Ordering::Equal,
    };

    cmp_start && cmp_end
}

/// Get a nested field value from an EncodedValue
fn get_nested_field<'a>(value: &'a EncodedValue, path: &[String]) -> Option<&'a EncodedValue> {
    if path.is_empty() {
        return Some(value);
    }

    if let EncodedValue::Struct(fields) = value {
        for (name, field_value) in fields {
            if name == &path[0] {
                return get_nested_field(field_value, &path[1..]);
            }
        }
    }

    None
}

/// Compare an EncodedValue to a KeyValue
fn compare_values(value: &EncodedValue, key: &KeyValue) -> std::cmp::Ordering {
    match (value, key) {
        (EncodedValue::Int32(v), KeyValue::Int32(k)) => v.cmp(k),
        (EncodedValue::Int64(v), KeyValue::Int64(k)) => v.cmp(k),
        (EncodedValue::Float32(v), KeyValue::Float32(k)) => v.partial_cmp(k).unwrap_or(std::cmp::Ordering::Equal),
        (EncodedValue::Float64(v), KeyValue::Float64(k)) => v.partial_cmp(k).unwrap_or(std::cmp::Ordering::Equal),
        (EncodedValue::Binary(v), KeyValue::String(k)) => {
            String::from_utf8_lossy(v).as_ref().cmp(k)
        }
        (EncodedValue::Boolean(v), KeyValue::Boolean(k)) => v.cmp(k),
        // Cross-type comparisons
        (EncodedValue::Int32(v), KeyValue::Int64(k)) => (*v as i64).cmp(k),
        (EncodedValue::Int64(v), KeyValue::Int32(k)) => v.cmp(&(*k as i64)),
        _ => std::cmp::Ordering::Equal, // Default for unsupported comparisons
    }
}

impl DataSource for VcfDataSource {
    fn row_type(&self) -> &EncodedType {
        &self.schema
    }

    fn globals(&self) -> Result<EncodedValue> {
        // VCF headers contain metadata that could be mapped to globals
        // For now, return empty struct
        Ok(EncodedValue::Struct(vec![]))
    }

    fn key_fields(&self) -> &[String] {
        // VCF logical keys are [locus, alleles] in Hail's representation
        // Return empty for now as we don't enforce key constraints
        &[]
    }

    fn num_partitions(&self) -> usize {
        // Single partition for now
        // Future: could partition by chromosome using index
        1
    }

    fn scan_partition(
        &self,
        partition_idx: usize,
        ranges: &[KeyRange],
    ) -> Result<Vec<EncodedValue>> {
        if partition_idx != 0 {
            return Ok(vec![]);
        }
        self.query_stream(ranges)?.collect()
    }

    fn query_stream(
        &self,
        ranges: &[KeyRange],
    ) -> Result<Box<dyn Iterator<Item = Result<EncodedValue>> + Send>> {
        // Try indexed query if we have an index and can build a region
        if let Some(index) = &self.index {
            if self.is_bgzf {
                if let Some(region) = self.ranges_to_region(ranges) {
                    // Check if this is a local file or remote
                    let is_local = !self.path.starts_with("gs://")
                        && !self.path.starts_with("s3://")
                        && !self.path.starts_with("http");

                    if is_local {
                        info!("Using local indexed query for region: {:?}", region);
                        return self.indexed_query_local(&region, index, ranges);
                    } else {
                        info!("Using remote indexed query for region: {:?}", region);
                        return self.indexed_query_remote(&region, index, ranges);
                    }
                }
            }
        }

        // Fall back to full scan with post-filtering
        debug!("Performing full VCF scan with post-filtering");
        let base_iter = self.full_scan_iter()?;

        if ranges.is_empty() {
            return Ok(base_iter);
        }

        // Apply in-memory filtering
        let ranges = ranges.to_vec();
        let filtered = base_iter.filter(move |result| match result {
            Ok(row) => row_matches_ranges(row, &ranges),
            Err(_) => true, // Pass through errors
        });

        Ok(Box::new(filtered))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ranges_to_region_contig_only() {
        // Create a minimal header
        let header: vcf::Header = "##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"
            .parse()
            .unwrap();
        let schema = super::super::schema::extract_schema_from_header(&header).unwrap();

        let source = VcfDataSource {
            path: "test.vcf".to_string(),
            header: Arc::new(header),
            schema,
            index: None,
            is_bgzf: false,
        };

        let ranges = vec![KeyRange {
            field_path: vec!["locus".to_string(), "contig".to_string()],
            start: QueryBound::Included(KeyValue::String("chr1".to_string())),
            end: QueryBound::Included(KeyValue::String("chr1".to_string())),
        }];

        let region = source.ranges_to_region(&ranges);
        assert!(region.is_some());
    }
}

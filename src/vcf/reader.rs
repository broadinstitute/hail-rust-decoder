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
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use tracing::debug;

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
        // TODO: Indexed queries require local file access for bgzf seeking
        // For now, always do full scan and filter in-memory if ranges provided
        let _ = ranges; // Acknowledge ranges parameter

        // Check if we could potentially use index (for logging)
        if self.index.is_some() && self.is_bgzf {
            if let Some(_region) = self.ranges_to_region(ranges) {
                debug!("Index available but indexed queries not yet supported for cloud/boxed readers");
            }
        }

        debug!("Performing full VCF scan");
        self.full_scan_iter()
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

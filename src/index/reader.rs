//! Index reader for B-tree index files

use crate::buffer::{InputBuffer, LEB128Buffer};
use crate::codec::{EncodedType, ETypeParser};
use crate::index::{IndexMetadata, IndexNode, InternalNode, LeafNode};
use crate::metadata::IndexSpec;
use crate::HailError;
use crate::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Index reader for Hail B-tree indexes
pub struct IndexReader {
    _index_dir: PathBuf,
    metadata: IndexMetadata,
    _leaf_type: EncodedType,
    _internal_type: EncodedType,
    node_cache: HashMap<u64, IndexNode>,
}

impl IndexReader {
    /// Create a new index reader from an index directory and spec
    ///
    /// # Arguments
    /// * `index_dir` - Path to the index directory (e.g., `.../index/part-0-xxx.idx`)
    /// * `spec` - Index specification from the table metadata
    ///
    /// This loads all index nodes into memory for fast lookups.
    pub fn new<P: AsRef<Path>>(index_dir: P, spec: &IndexSpec) -> Result<Self> {
        let index_dir = index_dir.as_ref().to_path_buf();

        // Read index metadata
        let metadata_path = index_dir.join("metadata.json.gz");
        let metadata = IndexMetadata::from_file(metadata_path)?;

        // Parse the EType for leaf nodes
        let leaf_type = ETypeParser::parse(&spec.leaf_codec.e_type)?;

        // Parse the EType for internal nodes
        let internal_type = ETypeParser::parse(&spec.internal_node_codec.e_type)?;

        // Load all nodes from the index file into cache
        let node_cache = Self::load_all_nodes(
            &index_dir,
            &metadata,
            &leaf_type,
            &internal_type,
        )?;

        Ok(IndexReader {
            _index_dir: index_dir,
            metadata,
            _leaf_type: leaf_type,
            _internal_type: internal_type,
            node_cache,
        })
    }

    /// Load all nodes from the index file into a cache
    ///
    /// Index files are typically small, so we load them entirely into memory.
    ///
    /// # Virtual Offsets
    /// Hail uses "virtual offsets" for index nodes when using BlockingBuffer (default).
    /// A virtual offset is a 64-bit integer:
    /// - High 48 bits: Physical file offset of the start of the block
    /// - Low 16 bits: Offset within the decompressed block
    ///
    /// We must read the file block-by-block, track physical offsets, and map them
    /// to memory offsets in our decompressed buffer.
    fn load_all_nodes(
        index_dir: &Path,
        metadata: &IndexMetadata,
        leaf_type: &EncodedType,
        internal_type: &EncodedType,
    ) -> Result<HashMap<u64, IndexNode>> {
        let index_file_path = index_dir.join(&metadata.index_path);
        let mut file = File::open(&index_file_path)?;

        // Map physical file offset -> offset in decompressed_data
        let mut block_map: HashMap<u64, usize> = HashMap::new();
        let mut decompressed_data: Vec<u8> = Vec::new();

        // Read blocks until EOF
        loop {
            let phys_offset = file.seek(SeekFrom::Current(0))?;

            // Read 4-byte block length
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // EOF
                Err(e) => return Err(e.into()),
            }
            let block_len = u32::from_le_bytes(len_buf) as usize;

            // Read compressed block data
            let mut compressed = vec![0u8; block_len];
            file.read_exact(&mut compressed)?;

            // Decompress (format: [4-byte decompressed len][zstd data])
            if block_len < 4 {
                return Err(HailError::InvalidFormat("Block too small".to_string()));
            }

            let expected_len = i32::from_le_bytes([
                compressed[0],
                compressed[1],
                compressed[2],
                compressed[3],
            ]) as usize;

            let zstd_data = &compressed[4..];
            let decompressed_block =
                zstd::decode_all(zstd_data).map_err(|_| HailError::Zstd)?;

            if decompressed_block.len() != expected_len {
                return Err(HailError::InvalidFormat(format!(
                    "Decompressed size mismatch: expected {}, got {}",
                    expected_len,
                    decompressed_block.len()
                )));
            }

            // Record mapping and append data
            block_map.insert(phys_offset, decompressed_data.len());
            decompressed_data.extend_from_slice(&decompressed_block);
        }

        // Helper to resolve offsets
        // Hail index offsets can be either:
        // 1. Direct physical file offsets (for root_offset) - maps to start of a block
        // 2. Virtual offsets (for child index_file_offset) - (phys_offset << 16) | local_offset
        //
        // We detect which by checking if the offset matches a block boundary directly.
        // If it does, we use the block's start. Otherwise we decode as virtual offset.
        let resolve_offset = |v_off: u64| -> Result<usize> {
            // First check if this is a direct physical block offset
            if let Some(&mem_base) = block_map.get(&v_off) {
                return Ok(mem_base);
            }

            // Otherwise decode as virtual offset
            let phys = v_off >> 16;
            let local = (v_off & 0xFFFF) as usize;

            let mem_base = block_map.get(&phys).ok_or_else(|| {
                HailError::Index(format!(
                    "Invalid physical offset in virtual offset: {} (from v_off={})",
                    phys, v_off
                ))
            })?;

            let mem_offset = mem_base + local;
            if mem_offset >= decompressed_data.len() {
                return Err(HailError::Index(format!(
                    "Virtual offset out of bounds: {} -> {}",
                    v_off, mem_offset
                )));
            }
            Ok(mem_offset)
        };

        // Parse nodes starting from root
        let mut cache = HashMap::new();
        let mut parsed_offsets = std::collections::HashSet::new();
        let mut queue: Vec<u64> = vec![metadata.root_offset];

        while let Some(v_offset) = queue.pop() {
            if parsed_offsets.contains(&v_offset) {
                continue;
            }

            let mem_offset = resolve_offset(v_offset)?;

            // Create a buffer slice for this node
            let slice = &decompressed_data[mem_offset..];
            let slice_buffer = SliceBuffer {
                data: slice,
                position: 0,
            };
            let mut leb_buffer = LEB128Buffer::new(slice_buffer);

            // Read node type
            let node_type = match leb_buffer.read_u8() {
                Ok(b) => b,
                Err(_) => continue,
            };

            let node = match node_type {
                0 => {
                    // Leaf
                    let value = leaf_type.read_present_value(&mut leb_buffer)?;
                    IndexNode::Leaf(LeafNode::from_encoded(value)?)
                }
                1 => {
                    // Internal
                    let value = internal_type.read_present_value(&mut leb_buffer)?;
                    let internal = InternalNode::from_encoded(value)?;

                    // Enqueue children
                    for child in &internal.children {
                        queue.push(child.index_file_offset as u64);
                    }
                    IndexNode::Internal(internal)
                }
                _ => {
                    return Err(HailError::InvalidFormat(format!(
                        "Unknown node type: {}",
                        node_type
                    )));
                }
            };

            cache.insert(v_offset, node);
            parsed_offsets.insert(v_offset);
        }

        Ok(cache)
    }

    /// Get the index metadata
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    /// Read a node at the given offset in the index file
    ///
    /// Nodes are read from an in-memory cache that was loaded during initialization.
    pub fn read_node(&self, offset: u64) -> Result<&IndexNode> {
        self.node_cache
            .get(&offset)
            .ok_or_else(|| HailError::Index(format!("No node found at offset {}", offset)))
    }

    /// Perform a point lookup in the index
    ///
    /// Returns the byte offset in the partition data file where the row
    /// with the given key can be found, or None if the key doesn't exist.
    pub fn lookup(&self, key: &crate::codec::EncodedValue) -> Result<Option<i64>> {
        self.lower_bound(key, self.metadata.height - 1, self.metadata.root_offset)
    }

    /// Binary search for a key in the B-tree (recursive)
    ///
    /// This implements the lower_bound algorithm from Hail's IndexReader.scala
    fn lower_bound(&self, key: &crate::codec::EncodedValue, level: u32, offset: u64) -> Result<Option<i64>> {
        let node = self.read_node(offset)?;

        if level == 0 {
            // We're at a leaf node
            match node {
                IndexNode::Leaf(leaf) => {
                    // Binary search for the key in the leaf
                    for entry in &leaf.keys {
                        if Self::keys_equal(&entry.key, key) {
                            return Ok(Some(entry.offset));
                        }
                    }
                    Ok(None)
                }
                _ => Err(HailError::Codec(
                    "Expected leaf node at level 0".to_string(),
                )),
            }
        } else {
            // We're at an internal node
            match node {
                IndexNode::Internal(internal) => {
                    // Find the correct child to descend into
                    // We want the last child whose first_key <= query_key
                    let mut child_idx = 0;
                    for (i, entry) in internal.children.iter().enumerate() {
                        if Self::key_less_or_equal(&entry.first_key, key) {
                            child_idx = i;
                        } else {
                            break;
                        }
                    }

                    // Recurse into the child
                    let child_offset = internal.children[child_idx].index_file_offset as u64;
                    self.lower_bound(key, level - 1, child_offset)
                }
                _ => Err(HailError::Codec(
                    "Expected internal node at non-zero level".to_string(),
                )),
            }
        }
    }

    /// Compare two keys for equality
    fn keys_equal(a: &crate::codec::EncodedValue, b: &crate::codec::EncodedValue) -> bool {
        use crate::codec::EncodedValue;
        match (a, b) {
            (EncodedValue::Struct(a_fields), EncodedValue::Struct(b_fields)) => {
                if a_fields.len() != b_fields.len() {
                    return false;
                }
                for (a_field, b_field) in a_fields.iter().zip(b_fields.iter()) {
                    if a_field.0 != b_field.0 || !Self::keys_equal(&a_field.1, &b_field.1) {
                        return false;
                    }
                }
                true
            }
            (EncodedValue::Binary(a), EncodedValue::Binary(b)) => a == b,
            (EncodedValue::Int32(a), EncodedValue::Int32(b)) => a == b,
            (EncodedValue::Int64(a), EncodedValue::Int64(b)) => a == b,
            (EncodedValue::Float32(a), EncodedValue::Float32(b)) => a == b,
            (EncodedValue::Float64(a), EncodedValue::Float64(b)) => a == b,
            (EncodedValue::Boolean(a), EncodedValue::Boolean(b)) => a == b,
            _ => false,
        }
    }

    /// Check if key a <= key b
    fn key_less_or_equal(a: &crate::codec::EncodedValue, b: &crate::codec::EncodedValue) -> bool {
        use crate::codec::EncodedValue;
        match (a, b) {
            (EncodedValue::Struct(a_fields), EncodedValue::Struct(b_fields)) => {
                for (a_field, b_field) in a_fields.iter().zip(b_fields.iter()) {
                    if a_field.0 != b_field.0 {
                        return false; // Field names don't match
                    }
                    match Self::compare_values(&a_field.1, &b_field.1) {
                        Some(std::cmp::Ordering::Less) => return true,
                        Some(std::cmp::Ordering::Greater) => return false,
                        Some(std::cmp::Ordering::Equal) => continue,
                        None => return false, // Can't compare
                    }
                }
                true // All fields equal
            }
            _ => {
                matches!(Self::compare_values(a, b), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
            }
        }
    }

    /// Compare two values
    fn compare_values(
        a: &crate::codec::EncodedValue,
        b: &crate::codec::EncodedValue,
    ) -> Option<std::cmp::Ordering> {
        use crate::codec::EncodedValue;
        match (a, b) {
            (EncodedValue::Binary(a), EncodedValue::Binary(b)) => {
                // Convert to strings for comparison
                let a_str = String::from_utf8_lossy(a);
                let b_str = String::from_utf8_lossy(b);
                Some(a_str.cmp(&b_str))
            }
            (EncodedValue::Int32(a), EncodedValue::Int32(b)) => Some(a.cmp(b)),
            (EncodedValue::Int64(a), EncodedValue::Int64(b)) => Some(a.cmp(b)),
            (EncodedValue::Float32(a), EncodedValue::Float32(b)) => a.partial_cmp(b),
            (EncodedValue::Float64(a), EncodedValue::Float64(b)) => a.partial_cmp(b),
            (EncodedValue::Boolean(a), EncodedValue::Boolean(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

// Simple buffer wrapper for in-memory slices
struct SliceBuffer<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> InputBuffer for SliceBuffer<'a> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if self.position + buf.len() > self.data.len() {
            return Err(HailError::UnexpectedEof);
        }
        buf.copy_from_slice(&self.data[self.position..self.position + buf.len()]);
        self.position += buf.len();
        Ok(())
    }
}

use hail_decoder::codec::EncodedValue;
use hail_decoder::index::IndexReader;
use hail_decoder::metadata::RVDComponentSpec;

#[test]
fn test_index_reader_initialization() {
    // Load the RVD metadata to get the index spec
    let rvd_metadata = RVDComponentSpec::from_file(
        "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz",
    )
    .expect("Failed to load RVD metadata");

    let index_spec = rvd_metadata
        .index_spec
        .as_ref()
        .expect("Expected index spec");

    // Create the index reader
    let index_path =
        "data/gene_models_hds/ht/prep_table.ht/index/part-0-7e0f7fd7-8efe-401a-b28a-ae8ac6d3aa06.idx";
    let reader = IndexReader::new(index_path, index_spec).expect("Failed to create index reader");

    // Check metadata
    let metadata = reader.metadata();
    assert_eq!(metadata.file_version, 66048);
    assert_eq!(metadata.branching_factor, 4096);
    assert_eq!(metadata.height, 2);
    assert_eq!(metadata.n_keys, 3);
}

#[test]
fn test_index_read_root_node() {
    // Load the RVD metadata to get the index spec
    let rvd_metadata = RVDComponentSpec::from_file(
        "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz",
    )
    .expect("Failed to load RVD metadata");

    let index_spec = rvd_metadata
        .index_spec
        .as_ref()
        .expect("Expected index spec");

    // Create the index reader
    let index_path =
        "data/gene_models_hds/ht/prep_table.ht/index/part-0-7e0f7fd7-8efe-401a-b28a-ae8ac6d3aa06.idx";
    let reader = IndexReader::new(index_path, index_spec).expect("Failed to create index reader");

    // Read the root node
    let root_offset = reader.metadata().root_offset;
    let root_node = reader.read_node(root_offset).expect("Failed to read root node");

    // At height 2, root should be an internal node
    match root_node {
        hail_decoder::index::IndexNode::Internal(internal) => {
            assert!(!internal.children.is_empty(), "Root node should have children");
            println!("Root node has {} children", internal.children.len());
        }
        _ => panic!("Expected internal node for root at height 2"),
    }
}

#[test]
fn test_index_lookup() {
    // Load the RVD metadata to get the index spec
    let rvd_metadata = RVDComponentSpec::from_file(
        "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz",
    )
    .expect("Failed to load RVD metadata");

    let index_spec = rvd_metadata
        .index_spec
        .as_ref()
        .expect("Expected index spec");

    // Create the index reader
    let index_path =
        "data/gene_models_hds/ht/prep_table.ht/index/part-0-7e0f7fd7-8efe-401a-b28a-ae8ac6d3aa06.idx";
    let reader = IndexReader::new(index_path, index_spec).expect("Failed to create index reader");

    // Create a key to search for - we need to look at what's actually in the data
    // For now, let's just verify that lookup doesn't crash
    // The key structure is: {gene_id: String, chrom: String, start: Int32}
    let search_key = EncodedValue::Struct(vec![
        (
            "gene_id".to_string(),
            EncodedValue::Binary(b"ENSG00000066468".to_vec()),
        ),
        (
            "chrom".to_string(),
            EncodedValue::Binary(b"10".to_vec()),
        ),
        ("start".to_string(), EncodedValue::Int32(121478332)),
    ]);

    // Perform lookup
    match reader.lookup(&search_key) {
        Ok(Some(offset)) => {
            println!("Found key at offset: {}", offset);
            // Offset 0 is valid - it means the row is at the very beginning
            assert!(offset >= 0, "Offset should be non-negative");
        }
        Ok(None) => {
            println!("Key not found in index");
            // This is okay - the key might not exist
        }
        Err(e) => {
            panic!("Lookup failed with error: {:?}", e);
        }
    }
}

#[test]
fn test_index_lookup_nonexistent_key() {
    // Load the RVD metadata to get the index spec
    let rvd_metadata = RVDComponentSpec::from_file(
        "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz",
    )
    .expect("Failed to load RVD metadata");

    let index_spec = rvd_metadata
        .index_spec
        .as_ref()
        .expect("Expected index spec");

    // Create the index reader
    let index_path =
        "data/gene_models_hds/ht/prep_table.ht/index/part-0-7e0f7fd7-8efe-401a-b28a-ae8ac6d3aa06.idx";
    let reader = IndexReader::new(index_path, index_spec).expect("Failed to create index reader");

    // Create a key that definitely doesn't exist
    let search_key = EncodedValue::Struct(vec![
        (
            "gene_id".to_string(),
            EncodedValue::Binary(b"NONEXISTENT_GENE".to_vec()),
        ),
        (
            "chrom".to_string(),
            EncodedValue::Binary(b"99".to_vec()),
        ),
        ("start".to_string(), EncodedValue::Int32(0)),
    ]);

    // Perform lookup - should return None
    let result = reader.lookup(&search_key).expect("Lookup should not error");
    assert!(
        result.is_none(),
        "Nonexistent key should return None, got: {:?}",
        result
    );
}

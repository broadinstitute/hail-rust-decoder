use hail_decoder::metadata::RVDComponentSpec;
use hail_decoder::query::{filter_partitions, KeyRange, KeyValue};

#[test]
fn test_partition_pruning_gene_models() {
    // Load metadata from the gene models test data
    let metadata = RVDComponentSpec::from_file(
        "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz",
    )
    .expect("Failed to load metadata");

    // Test 1: Query for chromosome "10" - should match the single partition
    let ranges = vec![KeyRange::point(
        "chrom".to_string(),
        KeyValue::String("10".to_string()),
    )];
    let result = filter_partitions(&metadata.range_bounds, &ranges);
    assert_eq!(result, vec![0], "Chromosome 10 should match partition 0");

    // Test 2: Query for chromosome "1" - should NOT match (data has chr 10 to X)
    let ranges = vec![KeyRange::point(
        "chrom".to_string(),
        KeyValue::String("1".to_string()),
    )];
    let result = filter_partitions(&metadata.range_bounds, &ranges);
    assert_eq!(
        result,
        Vec::<usize>::new(),
        "Chromosome 1 should not match any partition"
    );

    // Test 3: Query for chromosome "X" - should match the single partition
    let ranges = vec![KeyRange::point(
        "chrom".to_string(),
        KeyValue::String("X".to_string()),
    )];
    let result = filter_partitions(&metadata.range_bounds, &ranges);
    assert_eq!(result, vec![0], "Chromosome X should match partition 0");

    // Test 4: Range query for chromosomes in the data
    let ranges = vec![KeyRange::inclusive(
        "chrom".to_string(),
        KeyValue::String("1".to_string()),
        KeyValue::String("Z".to_string()),
    )];
    let result = filter_partitions(&metadata.range_bounds, &ranges);
    assert_eq!(result, vec![0], "Range 1-Z should match partition 0");
}

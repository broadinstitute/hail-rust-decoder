///! Test decoding our own generated simple Hail table
///! This table has interval + fields + exons array, all required (non-nullable)

use flate2::read::GzDecoder;
use hail_decoder::buffer::{BufferBuilder, InputBuffer};
use hail_decoder::codec::{EncodedType, EncodedValue, ETypeParser};
use serde_json::Value;
use std::fs::File;

#[test]
fn test_decode_06_interval_plus_array() {
    // Load the EType from our test table
    let metadata_path = "test_hail_data/06_interval_plus_array.ht/rows/metadata.json.gz";
    let metadata_file = File::open(metadata_path).expect("Failed to open metadata file");
    let decoder = GzDecoder::new(metadata_file);
    let metadata: Value = serde_json::from_reader(decoder).expect("Failed to parse metadata JSON");

    let etype_str = metadata["_codecSpec"]["_eType"]
        .as_str()
        .expect("Failed to get _eType from metadata");

    println!("\nEType: {}", etype_str);

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");

    // Open the data file
    let rows_dir = "test_hail_data/06_interval_plus_array.ht/rows/parts/";
    let entries: Vec<_> = std::fs::read_dir(rows_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("part-") && !name.ends_with(".crc")
        })
        .collect();

    // Find the partition with actual data (largest file)
    let part_file = entries.iter()
        .max_by_key(|e| e.metadata().unwrap().len())
        .expect("No data file found");

    println!("Reading from: {}", part_file.path().display());

    // Use BufferBuilder with LEB128 encoding
    let mut buffer = BufferBuilder::from_file(part_file.path())
        .expect("Failed to open data file")
        .with_leb128()
        .build();

    println!("\n=== Decoding row ===");

    // IMPORTANT: Even though EType says +EBaseStruct (required),
    // table rows ALWAYS have a present flag! (Hail quirk)
    let row_present = buffer.read_bool().expect("Failed to read row present flag");
    println!("Row present: {}", row_present);
    assert!(row_present, "Row should be present");

    // Decode the row struct
    let result = row_type.read_present_value(&mut buffer).expect("Failed to decode row");

    // Validate the decoded data
    if let EncodedValue::Struct(fields) = result {
        println!("\n✓ Successfully decoded {} fields", fields.len());

        // Check gene_id
        let gene_id = fields.iter()
            .find(|(name, _)| name == "gene_id")
            .and_then(|(_, val)| val.as_string());
        println!("gene_id: {:?}", gene_id);
        assert_eq!(gene_id, Some("ENSG00000066468".to_string()));

        // Check symbol
        let symbol = fields.iter()
            .find(|(name, _)| name == "symbol")
            .and_then(|(_, val)| val.as_string());
        println!("symbol: {:?}", symbol);
        assert_eq!(symbol, Some("FGFR2".to_string()));

        // Check exons array
        let exons = fields.iter()
            .find(|(name, _)| name == "exons")
            .map(|(_, val)| val);

        if let Some(EncodedValue::Array(exons_arr)) = exons {
            println!("\n✓ Exons array: {} elements", exons_arr.len());
            assert_eq!(exons_arr.len(), 3, "Expected 3 exons");

            // Check first exon
            if let EncodedValue::Struct(exon_fields) = &exons_arr[0] {
                let exon_type = exon_fields.iter()
                    .find(|(name, _)| name == "type")
                    .and_then(|(_, val)| val.as_string());
                println!("First exon type: {:?}", exon_type);
                assert_eq!(exon_type, Some("CDS".to_string()));
            }

            println!("\n✓✓✓ Successfully decoded complete table with array!");
        } else {
            panic!("Expected exons array");
        }
    } else {
        panic!("Expected struct value");
    }
}

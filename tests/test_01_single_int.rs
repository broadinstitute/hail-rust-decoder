///! Test decoding the simplest possible Hail table: single int32 field

use flate2::read::GzDecoder;
use hail_decoder::buffer::{BufferBuilder, InputBuffer};
use hail_decoder::codec::{EncodedType, EncodedValue, ETypeParser};
use serde_json::Value;
use std::fs::File;

#[test]
fn test_decode_01_single_int() {
    println!("\n=== Test 01: Single Int32 ===");

    // Load metadata
    let metadata_path = "test_hail_data/01_single_int.ht/rows/metadata.json.gz";
    let metadata_file = File::open(metadata_path).expect("Failed to open metadata file");
    let decoder = GzDecoder::new(metadata_file);
    let metadata: Value = serde_json::from_reader(decoder).expect("Failed to parse metadata");

    let etype_str = metadata["_codecSpec"]["_eType"]
        .as_str()
        .expect("Failed to get _eType");

    println!("EType: {}", etype_str);

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");

    // Find the data file
    let rows_dir = "test_hail_data/01_single_int.ht/rows/parts/";
    let entries: Vec<_> = std::fs::read_dir(rows_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("part-") && !name.ends_with(".crc")
        })
        .collect();

    // Find the file with actual data (larger than the empty ones)
    let part_file = entries.iter()
        .max_by_key(|e| e.metadata().unwrap().len())
        .expect("No data file found");

    println!("Reading from: {}", part_file.path().display());

    let mut buffer = BufferBuilder::from_file(part_file.path())
        .expect("Failed to open data file")
        .with_leb128()
        .build();

    println!("\nDecoding row...");

    // IMPORTANT: Even though EType says +EBaseStruct (required),
    // table rows ALWAYS have a present flag in the actual data!
    // This is a Hail quirk where + describes logical type, not physical encoding.
    let row_present = buffer.read_bool().expect("Failed to read row present flag");
    println!("Row present: {}", row_present);
    assert!(row_present, "Row should be present");

    // Now decode the row struct (skip the present flag since we already read it)
    let result = row_type.read_present_value(&mut buffer).expect("Failed to decode row");

    // Validate
    if let EncodedValue::Struct(fields) = result {
        println!("✓ Decoded {} field(s)", fields.len());

        let value = fields.iter()
            .find(|(name, _)| name == "value")
            .map(|(_, val)| val);

        if let Some(EncodedValue::Int32(v)) = value {
            println!("value: {}", v);
            assert_eq!(*v, 42, "Expected value to be 42");
            println!("\n✓✓✓ Test 01 PASSED!");
        } else {
            panic!("Expected Int32 value");
        }
    } else {
        panic!("Expected struct");
    }
}

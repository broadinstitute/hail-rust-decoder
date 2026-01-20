///! Test decoding a simple array

use flate2::read::GzDecoder;
use hail_decoder::buffer::BufferBuilder;
use hail_decoder::codec::{EncodedType, EncodedValue, ETypeParser};
use serde_json::Value;
use std::fs::File;

#[test]
fn test_decode_03_simple_array() {
    println!("\n=== Test 03: Simple Array ===");

    // Load metadata
    let metadata_path = "test_hail_data/03_simple_array.ht/rows/metadata.json.gz";
    let metadata_file = File::open(metadata_path).expect("Failed to open metadata file");
    let decoder = GzDecoder::new(metadata_file);
    let metadata: Value = serde_json::from_reader(decoder).expect("Failed to parse metadata");

    let etype_str = metadata["_codecSpec"]["_eType"]
        .as_str()
        .expect("Failed to get _eType");

    println!("EType: {}", etype_str);

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");

    // Find data file
    let rows_dir = "test_hail_data/03_simple_array.ht/rows/parts/";
    let entries: Vec<_> = std::fs::read_dir(rows_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("part-") && !name.ends_with(".crc")
        })
        .collect();

    let part_file = entries.iter()
        .max_by_key(|e| e.metadata().unwrap().len())
        .expect("No data file found");

    println!("Reading from: {}", part_file.path().display());

    let mut buffer = BufferBuilder::from_file(part_file.path())
        .expect("Failed to open data file")
        .with_leb128()
        .build();

    println!("\nDecoding row...");

    // Read row present flag (Hail quirk: always present even for +EBaseStruct)
    let row_present = buffer.read_bool().expect("Failed to read row present flag");
    println!("Row present: {}", row_present);
    assert!(row_present);

    // Decode the row struct
    let result = row_type.read_present_value(&mut buffer).expect("Failed to decode row");

    // Validate
    if let EncodedValue::Struct(fields) = result {
        println!("✓ Decoded {} field(s)", fields.len());

        // Check id field
        let id = fields.iter()
            .find(|(name, _)| name == "id")
            .and_then(|(_, val)| val.as_string());
        println!("id: {:?}", id);
        assert_eq!(id, Some("test1".to_string()));

        // Check numbers array
        let numbers = fields.iter()
            .find(|(name, _)| name == "numbers")
            .map(|(_, val)| val);

        if let Some(EncodedValue::Array(arr)) = numbers {
            println!("✓ Numbers array: {} elements", arr.len());
            assert_eq!(arr.len(), 3, "Expected 3 elements in array");

            // Check array values
            let values: Vec<i32> = arr.iter()
                .filter_map(|v| {
                    if let EncodedValue::Int32(n) = v {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect();

            println!("Array values: {:?}", values);
            assert_eq!(values, vec![1, 2, 3], "Expected [1, 2, 3]");

            println!("\n✓✓✓ Test 03 PASSED!");
        } else {
            panic!("Expected numbers array");
        }
    } else {
        panic!("Expected struct");
    }
}

use hail_decoder::buffer::{BufferBuilder, InputBuffer};
use hail_decoder::codec::{EncodedValue, ETypeParser};
use std::fs;

#[test]
fn test_07_nullable_array() {
    // Load metadata to get EType
    let metadata_path = "test_hail_data/07_nullable_array.ht/rows/metadata.json.gz";
    let metadata_bytes = fs::read(metadata_path).expect("Failed to read metadata file");

    let decoder = flate2::read::GzDecoder::new(&metadata_bytes[..]);
    let metadata: serde_json::Value = serde_json::from_reader(decoder)
        .expect("Failed to parse metadata JSON");

    let etype_str = metadata["_codecSpec"]["_eType"]
        .as_str()
        .expect("Failed to get EType string");

    println!("EType: {}", etype_str);

    // Parse EType
    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");
    println!("Successfully parsed EType");

    // Find the data partition with actual data (largest file)
    let parts_dir = "test_hail_data/07_nullable_array.ht/rows/parts";
    let entries: Vec<_> = fs::read_dir(parts_dir)
        .expect("Failed to read parts directory")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            name_str.starts_with("part-") && !name_str.starts_with(".")
        })
        .collect();

    println!("Found {} partition files", entries.len());

    // Get the largest partition file (contains actual data)
    let part_file = entries.iter()
        .max_by_key(|e| e.metadata().unwrap().len())
        .expect("No data file found");

    println!("Using partition: {:?} ({} bytes)",
             part_file.file_name(),
             part_file.metadata().unwrap().len());

    // Open file and create buffer stack
    let mut buffer = BufferBuilder::from_file(part_file.path())
        .expect("Failed to open data file")
        .with_leb128()
        .build();

    // IMPORTANT: Table rows ALWAYS have a present flag, even for +EBaseStruct
    println!("\n=== Decoding first row (id=test1, numbers=[1,2,3]) ===");
    let row_present = buffer.read_bool().expect("Failed to read row present flag");
    println!("Row present: {}", row_present);
    assert!(row_present, "Row should be present");

    // Decode the row struct (skip the present flag since we already read it)
    let result = row_type.read_present_value(&mut buffer).expect("Failed to decode row");
    println!("\n=== Decoded row ===");
    println!("{:#?}", result);

    // Verify structure
    if let EncodedValue::Struct(fields) = result {
        assert_eq!(fields.len(), 2, "Expected 2 fields");

        // Check id field
        let id_field = fields.iter().find(|(name, _)| name == "id").map(|(_, val)| val);
        if let Some(EncodedValue::Binary(id_bytes)) = id_field {
            let id_str = String::from_utf8(id_bytes.clone()).expect("Invalid UTF-8 in id");
            println!("  id: {}", id_str);
            assert_eq!(id_str, "test1", "Expected id to be 'test1'");
        } else {
            panic!("Expected id field to be Binary");
        }

        // Check numbers field - should be an array with 3 elements
        let numbers_field = fields.iter().find(|(name, _)| name == "numbers").map(|(_, val)| val);
        if let Some(EncodedValue::Array(numbers)) = numbers_field {
            println!("  numbers: {:?}", numbers);
            assert_eq!(numbers.len(), 3, "Expected array length 3");

            // Verify array contents
            for (i, expected) in [1i32, 2, 3].iter().enumerate() {
                if let EncodedValue::Int32(val) = numbers[i] {
                    assert_eq!(val, *expected, "Expected numbers[{}] = {}", i, expected);
                } else {
                    panic!("Expected Int32 at numbers[{}]", i);
                }
            }
        } else {
            panic!("Expected numbers field to be Array, got: {:?}", numbers_field);
        }

        println!("\n✓ Test passed! Decoded nullable array correctly.");
    } else {
        panic!("Expected Struct result");
    }

    // Now test the SECOND row which should have null array
    println!("\n=== Decoding second row (id=test2, numbers=null) ===");
    let row_present2 = buffer.read_bool().expect("Failed to read row 2 present flag");
    println!("Row 2 present: {}", row_present2);
    assert!(row_present2, "Row 2 should be present");

    let result2 = row_type.read_present_value(&mut buffer).expect("Failed to decode row 2");
    println!("\n=== Decoded row 2 ===");
    println!("{:#?}", result2);

    // Verify structure - numbers should be null
    if let EncodedValue::Struct(fields) = result2 {
        // Row 2 should only have 1 field (id), not 2, since numbers is null
        assert!(fields.len() == 1 || fields.len() == 2, "Expected 1 or 2 fields");

        // Check id field
        let id_field2 = fields.iter().find(|(name, _)| name == "id").map(|(_, val)| val);
        if let Some(EncodedValue::Binary(id_bytes)) = id_field2 {
            let id_str = String::from_utf8(id_bytes.clone()).expect("Invalid UTF-8 in id");
            println!("  id: {}", id_str);
            assert_eq!(id_str, "test2", "Expected id to be 'test2'");
        } else {
            panic!("Expected id field to be Binary");
        }

        // Check numbers field - should either be absent or be Null
        let numbers_field2 = fields.iter().find(|(name, _)| name == "numbers");
        if let Some((_, val)) = numbers_field2 {
            assert!(matches!(val, EncodedValue::Null),
                    "Expected numbers to be Null, got: {:?}", val);
            println!("  numbers: null");
        } else {
            println!("  numbers: (absent)");
        }

        println!("\n✓ Test passed! Decoded null array correctly.");
    } else {
        panic!("Expected Struct result");
    }
}

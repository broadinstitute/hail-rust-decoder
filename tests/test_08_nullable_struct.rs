use hail_decoder::buffer::{BufferBuilder, InputBuffer};
use hail_decoder::codec::{EncodedValue, ETypeParser};
use std::fs;

#[test]
fn test_08_nullable_struct() {
    let metadata_path = "test_hail_data/08_nullable_struct.ht/rows/metadata.json.gz";
    let metadata_bytes = fs::read(metadata_path).expect("Failed to read metadata");

    let decoder = flate2::read::GzDecoder::new(&metadata_bytes[..]);
    let metadata: serde_json::Value = serde_json::from_reader(decoder).expect("Failed to parse metadata");

    let etype_str = metadata["_codecSpec"]["_eType"].as_str().expect("Failed to get EType");
    println!("EType: {}", etype_str);

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");
    println!("Successfully parsed EType");

    // Find data partition
    let parts_dir = "test_hail_data/08_nullable_struct.ht/rows/parts";
    let entries: Vec<_> = fs::read_dir(parts_dir)
        .expect("Failed to read parts directory")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            name_str.starts_with("part-") && !name_str.starts_with(".")
        })
        .collect();

    let part_file = entries.iter()
        .max_by_key(|e| e.metadata().unwrap().len())
        .expect("No data file found");

    println!("Using partition: {:?}", part_file.file_name());

    let mut buffer = BufferBuilder::from_file(part_file.path())
        .expect("Failed to open data file")
        .with_leb128()
        .build();

    // Row present flag
    let row_present = buffer.read_bool().expect("Failed to read row present flag");
    println!("Row present: {}", row_present);
    assert!(row_present);

    // Decode row
    let result = row_type.read_present_value(&mut buffer).expect("Failed to decode row");
    println!("\n=== Decoded row ===");
    println!("{:#?}", result);

    // Verify
    if let EncodedValue::Struct(fields) = result {
        let id = fields.iter().find(|(n, _)| n == "id").map(|(_, v)| v);
        let info = fields.iter().find(|(n, _)| n == "info").map(|(_, v)| v);
        let value = fields.iter().find(|(n, _)| n == "value").map(|(_, v)| v);

        // Check id
        if let Some(EncodedValue::Binary(id_bytes)) = id {
            let id_str = String::from_utf8(id_bytes.clone()).unwrap();
            println!("id: {}", id_str);
            assert_eq!(id_str, "test1");
        }

        // Check info struct
        if let Some(EncodedValue::Struct(info_fields)) = info {
            println!("info struct has {} fields", info_fields.len());
            let name = info_fields.iter().find(|(n, _)| n == "name").map(|(_, v)| v);
            let age = info_fields.iter().find(|(n, _)| n == "age").map(|(_, v)| v);

            if let Some(EncodedValue::Binary(name_bytes)) = name {
                let name_str = String::from_utf8(name_bytes.clone()).unwrap();
                println!("  name: {}", name_str);
                assert_eq!(name_str, "Alice");
            }

            if let Some(EncodedValue::Int32(age_val)) = age {
                println!("  age: {}", age_val);
                assert_eq!(*age_val, 30);
            }
        } else {
            panic!("Expected info to be a struct");
        }

        // Check value
        if let Some(EncodedValue::Int32(v)) = value {
            println!("value: {}", v);
            assert_eq!(*v, 100);
        }

        println!("\n✓ Test passed!");
    }
}

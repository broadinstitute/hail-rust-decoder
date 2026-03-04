use genohype_core::buffer::{BufferBuilder, InputBuffer};
use genohype_core::codec::{EncodedValue, ETypeParser};
use std::fs;

#[test]
fn test_10_simple_dict() {
    let metadata_path = "test_hail_data/10_simple_dict.ht/rows/metadata.json.gz";
    let metadata_bytes = fs::read(metadata_path).expect("Failed to read metadata");

    let decoder = flate2::read::GzDecoder::new(&metadata_bytes[..]);
    let metadata: serde_json::Value = serde_json::from_reader(decoder).expect("Failed to parse metadata");

    let etype_str = metadata["_codecSpec"]["_eType"].as_str().expect("Failed to get EType");
    println!("EType: {}", etype_str);

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");
    println!("Successfully parsed EType");

    // Find all data partitions
    let parts_dir = "test_hail_data/10_simple_dict.ht/rows/parts";
    let mut entries: Vec<_> = fs::read_dir(parts_dir)
        .expect("Failed to read parts directory")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            name_str.starts_with("part-") && !name_str.starts_with(".")
        })
        .collect();

    // Sort by filename for consistent ordering
    entries.sort_by_key(|e| e.file_name());
    println!("Found {} partitions", entries.len());

    let mut rows_decoded = 0;

    // Read from all partitions
    for part_file in &entries {
        let metadata = part_file.metadata().unwrap();
        if metadata.len() == 0 {
            continue; // Skip empty partitions
        }

        println!("\nReading partition: {:?}", part_file.file_name());

        let mut buffer = BufferBuilder::from_file(part_file.path())
            .expect("Failed to open data file")
            .with_leb128()
            .build();

        // Decode rows from this partition
        loop {
            // Row present flag
            let row_present = match buffer.read_bool() {
                Ok(present) => present,
                Err(_) => break, // End of partition
            };

            if !row_present {
                break;
            }

            rows_decoded += 1;
            println!("\n=== Row {} ===", rows_decoded);

        // Decode row
        let result = row_type.read_present_value(&mut buffer).expect("Failed to decode row");
        println!("{:#?}", result);

        // Verify structure
        if let EncodedValue::Struct(fields) = result {
            let id = fields.iter().find(|(n, _)| n == "id").map(|(_, v)| v);
            let metadata_field = fields.iter().find(|(n, _)| n == "metadata").map(|(_, v)| v);
            let value = fields.iter().find(|(n, _)| n == "value").map(|(_, v)| v);

            // Check id
            let id_str = if let Some(EncodedValue::Binary(id_bytes)) = id {
                let s = String::from_utf8(id_bytes.clone()).unwrap();
                println!("id: {}", s);
                s
            } else {
                panic!("Expected id to be binary");
            };

            // Check metadata (Dict encoded as Array of Struct{key, value})
            if let Some(EncodedValue::Array(dict_array)) = metadata_field {
                println!("metadata (dict): {} entries", dict_array.len());

                // Convert to map for easier verification
                let mut dict_map = std::collections::HashMap::new();
                for entry in dict_array {
                    if let EncodedValue::Struct(kv_fields) = entry {
                        let key = kv_fields.iter().find(|(n, _)| n == "key").map(|(_, v)| v);
                        let value = kv_fields.iter().find(|(n, _)| n == "value").map(|(_, v)| v);

                        if let (Some(EncodedValue::Binary(k)), Some(EncodedValue::Binary(v))) = (key, value) {
                            let key_str = String::from_utf8(k.clone()).unwrap();
                            let val_str = String::from_utf8(v.clone()).unwrap();
                            dict_map.insert(key_str, val_str);
                        }
                    }
                }

                println!("  Dict entries: {:?}", dict_map);

                // Verify expected entries based on actual id
                match id_str.as_str() {
                    "test1" => {
                        assert_eq!(dict_map.len(), 3);
                        assert_eq!(dict_map.get("name"), Some(&"Alice".to_string()));
                        assert_eq!(dict_map.get("age"), Some(&"30".to_string()));
                        assert_eq!(dict_map.get("city"), Some(&"NYC".to_string()));
                    },
                    "test2" => {
                        assert_eq!(dict_map.len(), 2);
                        assert_eq!(dict_map.get("name"), Some(&"Bob".to_string()));
                        assert_eq!(dict_map.get("age"), Some(&"25".to_string()));
                    },
                    "test3" => {
                        assert_eq!(dict_map.len(), 1);
                        assert_eq!(dict_map.get("status"), Some(&"active".to_string()));
                    },
                    _ => panic!("Unexpected id: {}", id_str),
                }
            } else {
                panic!("Expected metadata to be an array (dict encoding)");
            }

            // Check value
            if let Some(EncodedValue::Int32(v)) = value {
                println!("value: {}", v);
                // Value corresponds to test number * 100
                let test_num = id_str.chars().last().unwrap().to_digit(10).unwrap() as i32;
                assert_eq!(*v, test_num * 100);
            }
        }
        } // end loop (rows in partition)
    } // end for (partitions)

    assert_eq!(rows_decoded, 3, "Expected to decode 3 rows");

    println!("\n✓ All rows decoded successfully!");
}

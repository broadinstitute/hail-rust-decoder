//! Debug test for row decoding issues
//!
//! This test traces through the row decoding process to identify
//! where the byte alignment goes wrong.

use hail_decoder::buffer::{BufferBuilder, InputBuffer};
use hail_decoder::codec::ETypeParser;
use std::fs;

#[test]
fn debug_gene_models_first_bytes() {
    let metadata_path = "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz";
    let metadata_bytes = fs::read(metadata_path).expect("Failed to read metadata");

    let decoder = flate2::read::GzDecoder::new(&metadata_bytes[..]);
    let metadata: serde_json::Value = serde_json::from_reader(decoder).expect("Failed to parse metadata");

    let etype_str = metadata["_codecSpec"]["_eType"].as_str().expect("Failed to get EType");
    println!("EType: {}", etype_str);

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");
    println!("Row type is_required: {}", row_type.is_required());

    // Open the partition file
    let part_path = "data/gene_models_hds/ht/prep_table.ht/rows/parts/part-0-7e0f7fd7-8efe-401a-b28a-ae8ac6d3aa06";
    let mut buffer = BufferBuilder::from_file(part_path)
        .expect("Failed to open partition file")
        .with_leb128()
        .build();

    // Read first 20 bytes and show them
    println!("\n=== First 20 bytes after decompression ===");
    let mut first_bytes = [0u8; 20];
    buffer.read_exact(&mut first_bytes).expect("Failed to read first bytes");

    println!("Bytes: {:02x?}", first_bytes);
    for (i, &b) in first_bytes.iter().enumerate() {
        println!("  byte[{}] = 0x{:02x} = {} (as bool: {})", i, b, b, b != 0);
    }

    // Interpret as potential values
    println!("\n=== Interpretations ===");

    // First byte as bool (row present flag)
    println!("byte[0] as bool (row present?): {}", first_bytes[0] != 0);

    // Bytes 1-4 as i32 (first binary length?)
    let len1 = i32::from_le_bytes([first_bytes[1], first_bytes[2], first_bytes[3], first_bytes[4]]);
    println!("bytes[1..5] as i32 (binary length?): {}", len1);

    // Bytes 0-3 as i32 (if no row present flag)
    let len0 = i32::from_le_bytes([first_bytes[0], first_bytes[1], first_bytes[2], first_bytes[3]]);
    println!("bytes[0..4] as i32 (if no present byte): {}", len0);
}

#[test]
fn debug_row_decode_step_by_step() {
    let metadata_path = "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz";
    let metadata_bytes = fs::read(metadata_path).expect("Failed to read metadata");

    let decoder = flate2::read::GzDecoder::new(&metadata_bytes[..]);
    let metadata: serde_json::Value = serde_json::from_reader(decoder).expect("Failed to parse metadata");

    let etype_str = metadata["_codecSpec"]["_eType"].as_str().expect("Failed to get EType");
    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");

    // Parse the struct fields to understand the schema
    if let hail_decoder::codec::EncodedType::EBaseStruct { required, fields } = &row_type {
        println!("Outer struct required: {}", required);
        println!("Number of fields: {}", fields.len());

        // Count nullable fields
        let nullable_fields: Vec<_> = fields.iter()
            .filter(|f| !f.encoded_type.is_required())
            .collect();

        println!("\nNullable fields ({}): ", nullable_fields.len());
        for f in &nullable_fields {
            println!("  {} : {:?}", f.name, f.encoded_type);
        }

        let bitmap_bytes = (nullable_fields.len() + 7) / 8;
        println!("\nExpected bitmap size: {} bytes", bitmap_bytes);
    }

    // Open the partition file
    let part_path = "data/gene_models_hds/ht/prep_table.ht/rows/parts/part-0-7e0f7fd7-8efe-401a-b28a-ae8ac6d3aa06";
    let mut buffer = BufferBuilder::from_file(part_path)
        .expect("Failed to open partition file")
        .with_leb128()
        .build();

    println!("\n=== Attempting to decode ===");

    // Try reading with row present flag first
    let row_present = buffer.read_bool().expect("Failed to read row present flag");
    println!("Row present flag: {}", row_present);

    if row_present {
        // Try to decode using read_present_value
        match row_type.read_present_value(&mut buffer) {
            Ok(value) => {
                println!("\n=== Successfully decoded row! ===");
                // Print first few fields
                if let hail_decoder::codec::EncodedValue::Struct(fields) = &value {
                    for (name, val) in fields.iter().take(5) {
                        println!("  {} = {:?}", name, val);
                    }
                    println!("  ... ({} total fields)", fields.len());
                }
            }
            Err(e) => {
                println!("Decode error: {:?}", e);
            }
        }
    }
}

#[test]
fn debug_simple_primitives_decode() {
    // Test with the simpler primitives table
    let metadata_path = "test_hail_data/02_primitives.ht/rows/metadata.json.gz";
    let metadata_bytes = fs::read(metadata_path).expect("Failed to read metadata");

    let decoder = flate2::read::GzDecoder::new(&metadata_bytes[..]);
    let metadata: serde_json::Value = serde_json::from_reader(decoder).expect("Failed to parse metadata");

    let etype_str = metadata["_codecSpec"]["_eType"].as_str().expect("Failed to get EType");
    println!("EType: {}", etype_str);

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");

    // Parse the struct fields
    if let hail_decoder::codec::EncodedType::EBaseStruct { required, fields } = &row_type {
        println!("Outer struct required: {}", required);

        let nullable_count = fields.iter().filter(|f| !f.encoded_type.is_required()).count();
        println!("Nullable fields: {}", nullable_count);

        for f in fields {
            println!("  {} : required={}", f.name, f.encoded_type.is_required());
        }
    }

    // Find a partition file
    let parts_dir = "test_hail_data/02_primitives.ht/rows/parts";
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

    println!("\nUsing partition: {:?}", part_file.file_name());

    let mut buffer = BufferBuilder::from_file(part_file.path())
        .expect("Failed to open partition file")
        .with_leb128()
        .build();

    // Read first 20 bytes
    println!("\n=== First 20 bytes ===");
    let mut first_bytes = [0u8; 20];
    buffer.read_exact(&mut first_bytes).expect("Failed to read");

    println!("Bytes: {:02x?}", first_bytes);
    println!("byte[0] as bool: {}", first_bytes[0] != 0);

    // Reopen and try decoding
    let mut buffer = BufferBuilder::from_file(part_file.path())
        .expect("Failed to open partition file")
        .with_leb128()
        .build();

    // This table has ALL required fields (no nullable), so:
    // - No bitmap needed for fields
    // But there should still be a row present flag

    let row_present = buffer.read_bool().expect("Failed to read row present");
    println!("\nRow present: {}", row_present);

    if row_present {
        match row_type.read_present_value(&mut buffer) {
            Ok(value) => {
                println!("\n=== Decoded row ===");
                if let hail_decoder::codec::EncodedValue::Struct(fields) = &value {
                    for (name, val) in fields {
                        println!("  {} = {:?}", name, val);
                    }
                }
            }
            Err(e) => {
                println!("Decode error: {:?}", e);
            }
        }
    }
}

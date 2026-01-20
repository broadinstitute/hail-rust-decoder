///! Test decoding actual data using parsed metadata
///!
///! This is the main integration test that validates:
///! 1. EType parsing from metadata
///! 2. Correct buffer stack setup (with LEB128)
///! 3. Complete row decoding with nested structs
///! 4. Array decoding

use flate2::read::GzDecoder;
use hail_decoder::buffer::{BufferBuilder, InputBuffer};
use hail_decoder::codec::{EncodedType, EncodedValue, ETypeParser};
use serde_json::Value;
use std::fs::File;

#[test]
fn test_decode_first_11_fields_using_metadata() {
    // Load the actual EType from metadata file
    let metadata_path = "data/gene_models_hds/ht/prep_table.ht/rows/metadata.json.gz";
    let metadata_file = File::open(metadata_path).expect("Failed to open metadata file");
    let decoder = GzDecoder::new(metadata_file);
    let metadata: Value = serde_json::from_reader(decoder).expect("Failed to parse metadata JSON");

    let etype_str = metadata["_codecSpec"]["_eType"]
        .as_str()
        .expect("Failed to get _eType from metadata");

    eprintln!("EType length: {} characters", etype_str.len());
    eprintln!("EType: {}", &etype_str[..100.min(etype_str.len())]);

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");

    eprintln!("Successfully parsed EType");

    println!("\n=== Decoded row structure ===");
    if let EncodedType::EBaseStruct { fields, .. } = &row_type {
        println!("Fields: {}", fields.len());
        let nullable_count = fields.iter().filter(|f| !f.encoded_type.is_required()).count();
        println!("Nullable fields: {}", nullable_count);
        println!("Expected bitmap bytes: {}", (nullable_count + 7) / 8);

        println!("\nField list:");
        for (i, field) in fields.iter().enumerate() {
            let req = if field.encoded_type.is_required() { "required" } else { "nullable" };
            println!("  {}: {} ({})", i, field.name, req);
        }
    }

    // Open the data file
    let rows_dir = "data/gene_models_hds/ht/prep_table.ht/rows/parts/";
    let entries: Vec<_> = std::fs::read_dir(rows_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("part-0")
                && !e.file_name().to_string_lossy().ends_with(".crc")
        })
        .collect();

    let part_file = &entries[0];

    println!("\n=== Setting up buffer with LEB128 encoding ===");

    // Use BufferBuilder to create the correct stack with LEB128 encoding
    // This is REQUIRED for proper integer decoding as specified in metadata
    let mut buffer = BufferBuilder::from_file(part_file.path())
        .expect("Failed to open data file")
        .with_leb128()
        .build();

    println!("=== Decoding first row ===");

    // IMPORTANT: Table rows are ALWAYS encoded with a present flag,
    // even when the EType is marked as +EBaseStruct (required).
    // This is a Hail quirk - the + describes the logical type, not the physical encoding.
    println!("Reading row present flag...");
    let row_present = buffer.read_bool().expect("Failed to read row present flag");
    println!("Row present: {}", row_present);
    if !row_present {
        panic!("Row is null!");
    }

    // Now decode the row struct (using read_present_value since we already checked the flag)
    let result = row_type.read_present_value(&mut buffer).expect("Failed to decode row");

    // Extract and print key fields
    if let EncodedValue::Struct(fields) = result {
        println!("\nDecoded {} fields", fields.len());

        // Print each field
        for (name, value) in &fields {
            match value {
                EncodedValue::Binary(bytes) => {
                    let s = String::from_utf8_lossy(bytes);
                    if s.len() < 50 {
                        println!("  {}: \"{}\"", name, s);
                    } else {
                        println!("  {}: <{} bytes>", name, bytes.len());
                    }
                }
                EncodedValue::Int32(v) => println!("  {}: {}", name, v),
                EncodedValue::Int64(v) => println!("  {}: {}", name, v),
                EncodedValue::Boolean(v) => println!("  {}: {}", name, v),
                EncodedValue::Array(arr) => println!("  {}: [array with {} elements]", name, arr.len()),
                EncodedValue::Struct(sub_fields) => println!("  {}: {{struct with {} fields}}", name, sub_fields.len()),
                EncodedValue::Null => println!("  {}: null", name),
                _ => println!("  {}: {:?}", name, value),
            }
        }

        // Validate specific fields
        let gene_id = fields.iter()
            .find(|(name, _)| name == "gene_id")
            .and_then(|(_, val)| val.as_string());

        println!("\n=== Validation ===");
        println!("gene_id: {:?}", gene_id);
        assert_eq!(gene_id, Some("ENSG00000066468".to_string()), "Expected FGFR2 gene ID");

        // Check exons array
        let exons = fields.iter()
            .find(|(name, _)| name == "exons")
            .map(|(_, val)| val);

        if let Some(EncodedValue::Array(exons_arr)) = exons {
            println!("exons: {} elements", exons_arr.len());
            assert_eq!(exons_arr.len(), 3, "Expected 3 exons for FGFR2");

            // Check first exon
            if let EncodedValue::Struct(exon_fields) = &exons_arr[0] {
                let feature_type = exon_fields.iter()
                    .find(|(name, _)| name == "feature_type")
                    .and_then(|(_, val)| val.as_string());

                println!("First exon feature_type: {:?}", feature_type);
                assert_eq!(feature_type, Some("CDS".to_string()), "Expected CDS feature type");
            }

            println!("\n✓ Successfully decoded and validated FGFR2 gene with exons!");
        } else {
            panic!("Expected exons array");
        }
    } else {
        panic!("Expected struct value");
    }
}

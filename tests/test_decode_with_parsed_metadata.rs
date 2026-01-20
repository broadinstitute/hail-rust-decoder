///! Test decoding actual data using parsed metadata

use hail_decoder::buffer::{InputBuffer, StreamBlockBuffer, ZstdBuffer};
use hail_decoder::codec::{EncodedType, EncodedValue, ETypeParser};
use std::fs::File;

#[test]
fn test_decode_first_11_fields_using_metadata() {
    // Parse the row structure from metadata
    // Simplified: just the first 11 fields up to and including exons
    let etype_str = "+EBaseStruct{interval:EBaseStruct{start:EBaseStruct{contig:+EBinary,position:+EInt32},end:EBaseStruct{contig:+EBinary,position:+EInt32},includesStart:+EBoolean,includesEnd:+EBoolean},gene_id:EBinary,gene_version:EBinary,gencode_symbol:EBinary,chrom:EBinary,strand:EBinary,start:EInt32,stop:EInt32,xstart:EInt64,xstop:EInt64,exons:EArray[EBaseStruct{feature_type:EBinary,start:EInt32,stop:EInt32,xstart:EInt64,xstop:EInt64}]}";

    let row_type = ETypeParser::parse(etype_str).expect("Failed to parse EType");

    println!("\n=== Decoded row structure ===");
    println!("Fields: {}", if let EncodedType::EBaseStruct { fields, .. } = &row_type {
        fields.len()
    } else {
        0
    });

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
    let file = File::open(part_file.path()).unwrap();
    let stream = StreamBlockBuffer::new(file);
    let mut buffer = ZstdBuffer::new(stream);

    println!("\n=== Decoding first row ===");

    // Read some bytes to see what we have
    let first_bytes: Vec<u8> = (0..20).map(|_| buffer.read_u8().unwrap()).collect();
    println!("First 20 bytes: {:02x?}", first_bytes);

    // Reopen file to start fresh
    let file2 = File::open(part_file.path()).unwrap();
    let stream2 = StreamBlockBuffer::new(file2);
    let mut buffer2 = ZstdBuffer::new(stream2);

    // Decode using the parsed type
    let result = row_type.read(&mut buffer2).expect("Failed to decode row");

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

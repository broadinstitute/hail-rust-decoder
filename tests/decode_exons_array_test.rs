///! Test decoding the exons array using the proper array decoder

use hail_decoder::buffer::{BlockingBuffer, InputBuffer, StreamBlockBuffer, ZstdBuffer};
use hail_decoder::codec::{EncodedType, EncodedValue};
use hail_decoder::codec::encoded_type::EncodedField;
use std::fs::File;

#[test]
fn test_decode_exons_array_proper() {
    // Set up buffer
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
    let zstd = ZstdBuffer::new(stream);
    let mut buffer = BlockingBuffer::with_default_size(zstd);

    // Define types
    let string_type = EncodedType::EBinary { required: true };

    // Define exon struct type
    // From metadata: feature_type:EBinary, start:EInt32, stop:EInt32, xstart:EInt64, xstop:EInt64
    let exon_struct = EncodedType::EBaseStruct {
        required: true, // Elements are required (struct itself)
        fields: vec![
            EncodedField {
                name: "feature_type".to_string(),
                encoded_type: EncodedType::EBinary { required: true },
                index: 0,
            },
            EncodedField {
                name: "start".to_string(),
                encoded_type: EncodedType::EInt32 { required: true },
                index: 1,
            },
            EncodedField {
                name: "stop".to_string(),
                encoded_type: EncodedType::EInt32 { required: true },
                index: 2,
            },
            EncodedField {
                name: "xstart".to_string(),
                encoded_type: EncodedType::EInt64 { required: true },
                index: 3,
            },
            EncodedField {
                name: "xstop".to_string(),
                encoded_type: EncodedType::EInt64 { required: true },
                index: 4,
            },
        ],
    };

    // exons: EArray[EBaseStruct{...}] (nullable)
    let exons_array_type = EncodedType::EArray {
        required: false, // Array itself is nullable
        element: Box::new(exon_struct),
    };

    println!("\n=== Decoding FGFR2 Gene with Exons Array ===\n");

    // Skip to exons array
    buffer.read_bool().unwrap(); // outer present
    let mut skip = [0u8; 5];
    buffer.read_exact(&mut skip).unwrap(); // mystery bytes

    // Skip interval
    string_type.read(&mut buffer).unwrap();
    buffer.read_i32().unwrap();
    string_type.read(&mut buffer).unwrap();
    buffer.read_i32().unwrap();
    buffer.read_bool().unwrap();
    buffer.read_bool().unwrap();

    // Skip primitive gene fields
    let gene_id = string_type.read(&mut buffer).unwrap();
    println!("Gene ID: {}", gene_id.as_string().unwrap());

    string_type.read(&mut buffer).unwrap(); // gene_version

    let symbol = string_type.read(&mut buffer).unwrap();
    println!("Gene Symbol: {}", symbol.as_string().unwrap());

    string_type.read(&mut buffer).unwrap(); // chrom
    string_type.read(&mut buffer).unwrap(); // strand
    buffer.read_i32().unwrap(); // start
    buffer.read_i32().unwrap(); // stop
    buffer.read_i64().unwrap(); // xstart
    buffer.read_i64().unwrap(); // xstop

    println!("\n=== Now decoding exons array ===");

    // Decode the exons array using our proper decoder
    let exons_result = exons_array_type.read(&mut buffer);

    match exons_result {
        Ok(EncodedValue::Array(exons)) => {
            println!("\n✓ Successfully decoded exons array!");
            println!("Number of exons: {}", exons.len());

            for (i, exon) in exons.iter().enumerate() {
                println!("\nExon {}:", i);

                if let EncodedValue::Struct(fields) = exon {
                    for (name, value) in fields {
                        match value {
                            EncodedValue::Binary(bytes) => {
                                println!("  {}: {}", name, String::from_utf8_lossy(bytes));
                            }
                            EncodedValue::Int32(v) => {
                                println!("  {}: {}", name, v);
                            }
                            EncodedValue::Int64(v) => {
                                println!("  {}: {}", name, v);
                            }
                            _ => {
                                println!("  {}: {:?}", name, value);
                            }
                        }
                    }
                } else {
                    println!("  Unexpected value: {:?}", exon);
                }
            }

            // Validate we got 3 exons (known from metadata)
            assert!(exons.len() > 0, "Should have at least one exon");

            // Validate first exon structure
            if let EncodedValue::Struct(fields) = &exons[0] {
                assert_eq!(fields.len(), 5, "Exon should have 5 fields");

                // Check field names
                assert_eq!(fields[0].0, "feature_type");
                assert_eq!(fields[1].0, "start");
                assert_eq!(fields[2].0, "stop");
                assert_eq!(fields[3].0, "xstart");
                assert_eq!(fields[4].0, "xstop");

                // Validate feature_type is a string
                if let EncodedValue::Binary(_) = &fields[0].1 {
                    // Good
                } else {
                    panic!("feature_type should be Binary");
                }

                println!("\n✓ Exon structure validated!");
            } else {
                panic!("First exon should be a Struct");
            }
        }
        Ok(EncodedValue::Null) => {
            panic!("Exons array is null, but we expected data!");
        }
        Ok(other) => {
            panic!("Expected array, got: {:?}", other);
        }
        Err(e) => {
            panic!("Failed to decode exons array: {:?}", e);
        }
    }
}

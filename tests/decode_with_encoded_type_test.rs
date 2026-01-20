///! Test decoding using EncodedType

use hail_decoder::buffer::{InputBuffer, StreamBlockBuffer, ZstdBuffer};
use hail_decoder::codec::{EncodedType, EncodedValue};
use std::fs::File;

#[test]
fn test_decode_simple_string() {
    // Test decoding a required binary/string with byte-length prefix
    let mut data = Vec::new();

    // Stream block: [length][data]
    let string_data = vec![
        0x04, // length = 4
        0x30, 0x2e, 0x39, 0x35, // "0.95"
    ];

    data.extend_from_slice(&(string_data.len() as u32).to_le_bytes());
    data.extend_from_slice(&string_data);

    let mut buffer = StreamBlockBuffer::new(&data[..]);

    let etype = EncodedType::EBinary { required: true };
    let value = etype.read(&mut buffer).unwrap();

    match value {
        EncodedValue::Binary(bytes) => {
            assert_eq!(bytes, b"0.95");
            let s = String::from_utf8(bytes).unwrap();
            assert_eq!(s, "0.95");
        }
        _ => panic!("Expected Binary value"),
    }
}

#[test]
fn test_decode_nullable_string() {
    // Test decoding a nullable binary/string
    let mut data = Vec::new();

    let string_data = vec![
        0x01, // present = true
        0x05, // length = 5
        0x68, 0x65, 0x6c, 0x6c, 0x6f, // "hello"
    ];

    data.extend_from_slice(&(string_data.len() as u32).to_le_bytes());
    data.extend_from_slice(&string_data);

    let mut buffer = StreamBlockBuffer::new(&data[..]);

    let etype = EncodedType::EBinary { required: false };
    let value = etype.read(&mut buffer).unwrap();

    assert_eq!(value.as_string(), Some("hello".to_string()));
}

#[test]
fn test_decode_null_string() {
    // Test decoding a null string
    let mut data = Vec::new();

    let string_data = vec![
        0x00, // present = false
    ];

    data.extend_from_slice(&(string_data.len() as u32).to_le_bytes());
    data.extend_from_slice(&string_data);

    let mut buffer = StreamBlockBuffer::new(&data[..]);

    let etype = EncodedType::EBinary { required: false };
    let value = etype.read(&mut buffer).unwrap();

    assert_eq!(value, EncodedValue::Null);
}

#[test]
fn test_decode_row_start() {
    // Try decoding the first few fields of the actual row data
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
    let mut zstd = ZstdBuffer::new(stream);

    // The row starts with a present flag for the outer struct
    let outer_present = zstd.read_bool().unwrap();
    assert_eq!(outer_present, true, "Outer struct should be present");

    // Skip some bytes that we don't understand yet (possibly interval data)
    let mut skip_bytes = [0u8; 5];
    zstd.read_exact(&mut skip_bytes).unwrap();
    println!("Skipped bytes: {:02x?}", skip_bytes);

    // Now we should have the first string: "chr10" (chromosome in the interval)
    let etype = EncodedType::EBinary { required: true };
    let chr1 = etype.read(&mut zstd).unwrap();
    println!("First chr: {:?}", chr1.as_string());

    // There's a position (i32)
    let pos1 = zstd.read_i32().unwrap();
    println!("First position: {}", pos1);

    // Another chr (end of interval)
    let chr2 = etype.read(&mut zstd).unwrap();
    println!("Second chr: {:?}", chr2.as_string());

    // Another position
    let pos2 = zstd.read_i32().unwrap();
    println!("Second position: {}", pos2);

    // Two boolean flags (includes_start, includes_end)
    let inc_start = zstd.read_bool().unwrap();
    let inc_end = zstd.read_bool().unwrap();
    println!("Interval flags: start={}, end={}", inc_start, inc_end);

    // Now we should have gene_id: "ENSG00000066468"
    let gene_id = etype.read(&mut zstd).unwrap();
    println!("Gene ID: {:?}", gene_id.as_string());
    assert_eq!(gene_id.as_string(), Some("ENSG00000066468".to_string()));

    // Gene version: "24"
    let gene_version = etype.read(&mut zstd).unwrap();
    println!("Gene version: {:?}", gene_version.as_string());
    assert_eq!(gene_version.as_string(), Some("24".to_string()));

    // Symbol: "FGFR2"
    let symbol = etype.read(&mut zstd).unwrap();
    println!("Symbol: {:?}", symbol.as_string());
    assert_eq!(symbol.as_string(), Some("FGFR2".to_string()));

    println!("\n✓ Successfully decoded first gene's basic fields!");
}

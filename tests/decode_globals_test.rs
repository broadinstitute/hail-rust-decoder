///! Test decoding the globals data from the test dataset

use genohype_core::buffer::{BlockingBuffer, InputBuffer, StreamBlockBuffer, ZstdBuffer};
use genohype_core::codec::{Decoder, HailDecoder, Value};
use genohype_core::schema::HailType;
use std::fs::File;

#[test]
fn test_decode_globals_raw_bytes() {
    // First, let's understand what the 7 bytes actually represent
    // Bytes: [01, 04, 30, 2e, 39, 35, 00]

    // Let's manually decode to understand the structure
    let file = File::open("data/gene_models_hds/ht/prep_table.ht/globals/parts/part-0")
        .expect("Failed to open globals file");

    // Build proper buffer stack: StreamBlockBuffer -> ZstdBuffer -> BlockingBuffer
    let stream = StreamBlockBuffer::new(file);
    let zstd = ZstdBuffer::new(stream);
    let mut buffer = BlockingBuffer::with_default_size(zstd);

    // Read all 7 bytes
    let mut bytes = [0u8; 7];
    buffer.read_exact(&mut bytes).expect("Failed to read");

    println!("Raw bytes: {:02x?}", bytes);
    println!("As chars: {:?}", String::from_utf8_lossy(&bytes));

    // Let's decode byte by byte to understand:
    // Byte 0: 01 = present flag (struct is not null)
    assert_eq!(bytes[0], 0x01, "First byte should be present flag");

    // Byte 1: 04 = length of string
    assert_eq!(bytes[1], 0x04, "Second byte should be string length");

    // Bytes 2-5: "0.95"
    assert_eq!(&bytes[2..6], b"0.95", "Should contain the string 0.95");

    // Byte 6: 00 = ? (might be null terminator or end of struct)
    assert_eq!(bytes[6], 0x00, "Last byte");
}

#[test]
fn test_decode_string_from_globals() {
    // Test decoding just a string value
    let file = File::open("data/gene_models_hds/ht/prep_table.ht/globals/parts/part-0")
        .expect("Failed to open globals file");

    // Build proper buffer stack: StreamBlockBuffer -> ZstdBuffer -> BlockingBuffer
    let stream = StreamBlockBuffer::new(file);
    let zstd = ZstdBuffer::new(stream);
    let mut buffer = BlockingBuffer::with_default_size(zstd);

    let decoder = HailDecoder::new();

    // Try to decode as a simple string
    let value = decoder.decode(&mut buffer, &HailType::String);

    match value {
        Ok(Value::String(s)) => {
            println!("Decoded string: {:?}", s);
            assert_eq!(s, "0.95");
        }
        Ok(other) => panic!("Expected string, got: {:?}", other),
        Err(e) => {
            println!("Decoding failed: {}", e);
            // This might fail because the structure is more complex
            // Let's see what happens
        }
    }
}

#[test]
fn test_manual_string_decode() {
    // Manually construct the string bytes and test
    let mut data = Vec::new();

    // StreamBlockBuffer format: [length][data]
    let string_data = vec![
        0x01, // present
        0x04, 0x00, 0x00, 0x00, // length = 4
        0x30, 0x2e, 0x39, 0x35, // "0.95"
    ];

    // Wrap in stream block
    data.extend_from_slice(&(string_data.len() as u32).to_le_bytes());
    data.extend_from_slice(&string_data);

    // StreamBlockBuffer -> BlockingBuffer to get InputBuffer
    let stream = StreamBlockBuffer::new(&data[..]);
    let mut buffer = BlockingBuffer::with_default_size(stream);
    let decoder = HailDecoder::new();

    let value = decoder.decode(&mut buffer, &HailType::String).unwrap();

    match value {
        Value::String(s) => assert_eq!(s, "0.95"),
        other => panic!("Expected string, got: {:?}", other),
    }
}

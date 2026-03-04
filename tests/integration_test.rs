//! Integration tests for the Hail decoder
//!
//! These tests use real Hail table data from the test dataset.

use genohype_core::buffer::{BlockingBuffer, InputBuffer, StreamBlockBuffer, ZstdBuffer};
use std::fs::File;

#[test]
fn test_read_globals_file() {
    // The globals file is the simplest test case:
    // - 41 bytes total
    // - 2 Zstd blocks
    // - Block 1: 20 bytes compressed -> 7 bytes decompressed
    // - Block 2: 13 bytes compressed -> 0 bytes decompressed (empty)
    // - Expected content: "0.95" (string) encoded in Hail format

    let file = File::open("data/gene_models_hds/ht/prep_table.ht/globals/parts/part-0")
        .expect("Failed to open globals file - make sure test data exists");

    // Build proper buffer stack: StreamBlockBuffer -> ZstdBuffer -> BlockingBuffer
    let stream = StreamBlockBuffer::new(file);
    let zstd = ZstdBuffer::new(stream);
    let mut buffer = BlockingBuffer::with_default_size(zstd);

    // Read the first 7 bytes of decompressed data (all available data)
    let mut buf = [0u8; 7];
    buffer.read_exact(&mut buf)
        .expect("Failed to read from buffer");

    // The exact bytes depend on the Hail encoding format
    // Expected: [01, 04, 30, 2e, 39, 35, 00] which is "\u{1}\u{4}0.95\0"
    println!("All 7 bytes (hex): {:02x?}", buf);
    println!(
        "All 7 bytes (ascii-ish): {:?}",
        String::from_utf8_lossy(&buf)
    );

    // Verify we got the expected data
    assert_eq!(buf, [0x01, 0x04, 0x30, 0x2e, 0x39, 0x35, 0x00]);

    // Verify that reading more returns EOF
    let mut extra = [0u8; 1];
    let result = buffer.read_exact(&mut extra);
    assert!(
        matches!(result, Err(hail_decoder::error::HailError::UnexpectedEof)),
        "Expected EOF after reading all data"
    );
}

#[test]
fn test_read_all_globals_data() {
    // Read all decompressed data from the globals file
    let file = File::open("data/gene_models_hds/ht/prep_table.ht/globals/parts/part-0")
        .expect("Failed to open globals file");

    // Build proper buffer stack: StreamBlockBuffer -> ZstdBuffer -> BlockingBuffer
    let stream = StreamBlockBuffer::new(file);
    let zstd = ZstdBuffer::new(stream);
    let mut buffer = BlockingBuffer::with_default_size(zstd);

    // According to the actual file:
    // Block 1: 7 bytes decompressed (contains "0.95" string data)
    // Block 2: 0 bytes decompressed (empty block)
    // Total: 7 bytes
    let mut all_data = Vec::new();
    let mut temp_buf = [0u8; 1];

    // Read until we hit EOF
    loop {
        match buffer.read_exact(&mut temp_buf) {
            Ok(()) => all_data.push(temp_buf[0]),
            Err(hail_decoder::error::HailError::UnexpectedEof) => break,
            Err(e) => panic!("Unexpected error: {}", e),
        }

        // Safety check to avoid infinite loop
        if all_data.len() > 100 {
            panic!("Read too much data - expected ~8 bytes");
        }
    }

    println!("Total decompressed bytes: {}", all_data.len());
    println!("Decompressed data (hex): {:02x?}", all_data);
    println!(
        "Decompressed data (ascii-ish): {:?}",
        String::from_utf8_lossy(&all_data)
    );

    // Should be exactly 7 bytes (Block 2 has size 0)
    assert_eq!(
        all_data.len(),
        7,
        "Expected 7 bytes of decompressed data"
    );
}

#[test]
fn test_read_rows_file() {
    // Test with a rows data file (larger, more realistic)
    // Find the first rows part file
    let rows_dir = "data/gene_models_hds/ht/prep_table.ht/rows/parts/";

    let entries = std::fs::read_dir(rows_dir)
        .expect("Failed to read rows directory")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("part-0")
                && !e.file_name().to_string_lossy().ends_with(".crc")
        })
        .collect::<Vec<_>>();

    assert!(
        !entries.is_empty(),
        "No part files found in rows directory"
    );

    let part_file = &entries[0];
    println!("Testing with file: {:?}", part_file.file_name());

    let file = File::open(part_file.path()).expect("Failed to open rows file");
    // Build proper buffer stack: StreamBlockBuffer -> ZstdBuffer -> BlockingBuffer
    let stream = StreamBlockBuffer::new(file);
    let zstd = ZstdBuffer::new(stream);
    let mut buffer = BlockingBuffer::with_default_size(zstd);

    // Read first 64 bytes of decompressed data
    let mut buf = [0u8; 64];
    buffer.read_exact(&mut buf)
        .expect("Failed to read from rows file");

    println!("First 64 bytes from rows file (hex): {:02x?}", buf);

    // Basic sanity check
    assert!(buf.iter().any(|&b| b != 0), "Expected non-zero data");
}

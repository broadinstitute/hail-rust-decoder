///! Test decoding row data from the test dataset
///!
///! The test data contains 3 genes. Let's decode the first one to understand the format.

use genohype_core::buffer::{BlockingBuffer, InputBuffer, StreamBlockBuffer, ZstdBuffer};
use std::fs::File;

#[test]
fn test_examine_row_bytes() {
    // Open the rows file and look at the first bytes
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
    println!("Reading from: {:?}", part_file.file_name());

    let file = File::open(part_file.path()).unwrap();
    let stream = StreamBlockBuffer::new(file);
    let zstd = ZstdBuffer::new(stream);
    let mut buffer = BlockingBuffer::with_default_size(zstd);

    // Read first 200 bytes to analyze structure
    let mut bytes = vec![0u8; 200];
    buffer.read_exact(&mut bytes).unwrap();

    println!("\nFirst 200 bytes:");
    for (i, chunk) in bytes.chunks(16).enumerate() {
        print!("{:04x}: ", i * 16);
        for b in chunk {
            print!("{:02x} ", b);
        }
        print!(" | ");
        for b in chunk {
            if b.is_ascii_graphic() || *b == b' ' {
                print!("{}", *b as char);
            } else {
                print!(".");
            }
        }
        println!();
    }

    // Look for recognizable strings
    let data_str = String::from_utf8_lossy(&bytes);
    println!("\nSearching for known gene symbols...");

    if data_str.contains("chr10") {
        println!("✓ Found 'chr10'");
    }
    if data_str.contains("FGFR2") {
        println!("✓ Found 'FGFR2'");
    }
    if data_str.contains("ENSG00000066468") {
        println!("✓ Found 'ENSG00000066468'");
    }
}

#[test]
fn test_manually_decode_first_fields() {
    // Based on the metadata, the row structure starts with:
    // - interval: Interval[Locus(GRCh38)]
    // - gene_id: String
    // - gene_version: String
    // - gencode_symbol: String
    // - chrom: String
    // - strand: String
    // - start: Int32
    // - stop: Int32

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

    // Try to manually parse the first row
    println!("\nManual decoding attempt:");

    // First, there might be a row count or similar header
    // Let's read a few initial integers to see
    let val1 = buffer.read_i32().unwrap();
    println!("First i32: {} (0x{:08x})", val1, val1);

    let val2 = buffer.read_i32().unwrap();
    println!("Second i32: {} (0x{:08x})", val2, val2);

    // Try reading as a string (length-prefixed)
    let len = buffer.read_i32().unwrap();
    println!("Third value as length: {} (0x{:08x})", len, len);

    if len > 0 && len < 100 {
        let mut str_bytes = vec![0u8; len as usize];
        buffer.read_exact(&mut str_bytes).unwrap();
        println!("String value: {:?}", String::from_utf8_lossy(&str_bytes));
    }
}

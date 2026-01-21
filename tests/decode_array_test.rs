///! Test decoding arrays from row data

use hail_decoder::buffer::{BlockingBuffer, InputBuffer, StreamBlockBuffer, ZstdBuffer};
use hail_decoder::codec::{EncodedType, EncodedValue};
use std::fs::File;

#[test]
fn test_decode_exons_array() {
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

    let string_type = EncodedType::EBinary { required: true };

    // Skip to after xstop
    buffer.read_bool().unwrap(); // outer present
    let mut skip = [0u8; 5];
    buffer.read_exact(&mut skip).unwrap(); // mystery

    // Skip interval
    string_type.read(&mut buffer).unwrap();
    buffer.read_i32().unwrap();
    string_type.read(&mut buffer).unwrap();
    buffer.read_i32().unwrap();
    buffer.read_bool().unwrap();
    buffer.read_bool().unwrap();

    // Skip gene fields
    string_type.read(&mut buffer).unwrap(); // gene_id
    string_type.read(&mut buffer).unwrap(); // gene_version
    string_type.read(&mut buffer).unwrap(); // symbol
    string_type.read(&mut buffer).unwrap(); // chrom
    string_type.read(&mut buffer).unwrap(); // strand
    buffer.read_i32().unwrap(); // start
    buffer.read_i32().unwrap(); // stop
    buffer.read_i64().unwrap(); // xstart
    buffer.read_i64().unwrap(); // xstop

    println!("\n=== At array position ===");

    // Read the next bytes to see what we have
    let mut peek = [0u8; 10];
    buffer.read_exact(&mut peek).unwrap();
    println!("Next 10 bytes: {:02x?}", peek);

    // Reset by recreating buffer (hacky but works for test)
    let file2 = File::open(part_file.path()).unwrap();
    let stream2 = StreamBlockBuffer::new(file2);
    let zstd2 = ZstdBuffer::new(stream2);
    let mut buffer2 = BlockingBuffer::with_default_size(zstd2);

    // Skip again to same position
    buffer2.read_bool().unwrap();
    let mut skip2 = [0u8; 5];
    buffer2.read_exact(&mut skip2).unwrap();
    string_type.read(&mut buffer2).unwrap();
    buffer2.read_i32().unwrap();
    string_type.read(&mut buffer2).unwrap();
    buffer2.read_i32().unwrap();
    buffer2.read_bool().unwrap();
    buffer2.read_bool().unwrap();
    string_type.read(&mut buffer2).unwrap();
    string_type.read(&mut buffer2).unwrap();
    string_type.read(&mut buffer2).unwrap();
    string_type.read(&mut buffer2).unwrap();
    string_type.read(&mut buffer2).unwrap();
    buffer2.read_i32().unwrap();
    buffer2.read_i32().unwrap();
    buffer2.read_i64().unwrap();
    buffer2.read_i64().unwrap();

    // Try different interpretations
    println!("\n=== Attempting array decode ===");

    // Theory 1: Maybe there are 5 more mystery bytes?
    let mut mystery2 = [0u8; 5];
    buffer2.read_exact(&mut mystery2).unwrap();
    println!("Potential mystery bytes: {:02x?}", mystery2);

    // Now try reading as array with byte length
    let array_len = buffer2.read_u8().unwrap();
    println!("Array length (as u8): {}", array_len);

    if array_len > 0 && array_len < 100 {
        println!("\nDecoding {} exons:", array_len);

        for i in 0..array_len {
            println!("\nExon {}:", i);
            let feature = string_type.read(&mut buffer2).unwrap();
            println!("  feature_type: {}", feature.as_string().unwrap());

            let start = buffer2.read_i32().unwrap();
            println!("  start: {}", start);

            let stop = buffer2.read_i32().unwrap();
            println!("  stop: {}", stop);

            let xstart = buffer2.read_i64().unwrap();
            println!("  xstart: {}", xstart);

            let xstop = buffer2.read_i64().unwrap();
            println!("  xstop: {}", xstop);

            // Check for separator
            let sep = buffer2.read_u8().unwrap();
            println!("  separator: 0x{:02x}", sep);
        }

        println!("\n✓ Successfully decoded exons array!");
    }
}

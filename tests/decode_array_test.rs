///! Test decoding arrays from row data

use hail_decoder::buffer::{InputBuffer, StreamBlockBuffer, ZstdBuffer};
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
    let mut zstd = ZstdBuffer::new(stream);

    let string_type = EncodedType::EBinary { required: true };

    // Skip to after xstop
    zstd.read_bool().unwrap(); // outer present
    let mut skip = [0u8; 5];
    zstd.read_exact(&mut skip).unwrap(); // mystery

    // Skip interval
    string_type.read(&mut zstd).unwrap();
    zstd.read_i32().unwrap();
    string_type.read(&mut zstd).unwrap();
    zstd.read_i32().unwrap();
    zstd.read_bool().unwrap();
    zstd.read_bool().unwrap();

    // Skip gene fields
    string_type.read(&mut zstd).unwrap(); // gene_id
    string_type.read(&mut zstd).unwrap(); // gene_version
    string_type.read(&mut zstd).unwrap(); // symbol
    string_type.read(&mut zstd).unwrap(); // chrom
    string_type.read(&mut zstd).unwrap(); // strand
    zstd.read_i32().unwrap(); // start
    zstd.read_i32().unwrap(); // stop
    zstd.read_i64().unwrap(); // xstart
    zstd.read_i64().unwrap(); // xstop

    println!("\n=== At array position ===");

    // Read the next bytes to see what we have
    let mut peek = [0u8; 10];
    zstd.read_exact(&mut peek).unwrap();
    println!("Next 10 bytes: {:02x?}", peek);

    // Reset by recreating buffer (hacky but works for test)
    let file2 = File::open(part_file.path()).unwrap();
    let stream2 = StreamBlockBuffer::new(file2);
    let mut zstd2 = ZstdBuffer::new(stream2);

    // Skip again to same position
    zstd2.read_bool().unwrap();
    let mut skip2 = [0u8; 5];
    zstd2.read_exact(&mut skip2).unwrap();
    string_type.read(&mut zstd2).unwrap();
    zstd2.read_i32().unwrap();
    string_type.read(&mut zstd2).unwrap();
    zstd2.read_i32().unwrap();
    zstd2.read_bool().unwrap();
    zstd2.read_bool().unwrap();
    string_type.read(&mut zstd2).unwrap();
    string_type.read(&mut zstd2).unwrap();
    string_type.read(&mut zstd2).unwrap();
    string_type.read(&mut zstd2).unwrap();
    string_type.read(&mut zstd2).unwrap();
    zstd2.read_i32().unwrap();
    zstd2.read_i32().unwrap();
    zstd2.read_i64().unwrap();
    zstd2.read_i64().unwrap();

    // Try different interpretations
    println!("\n=== Attempting array decode ===");

    // Theory 1: Maybe there are 5 more mystery bytes?
    let mut mystery2 = [0u8; 5];
    zstd2.read_exact(&mut mystery2).unwrap();
    println!("Potential mystery bytes: {:02x?}", mystery2);

    // Now try reading as array with byte length
    let array_len = zstd2.read_u8().unwrap();
    println!("Array length (as u8): {}", array_len);

    if array_len > 0 && array_len < 100 {
        println!("\nDecoding {} exons:", array_len);

        for i in 0..array_len {
            println!("\nExon {}:", i);
            let feature = string_type.read(&mut zstd2).unwrap();
            println!("  feature_type: {}", feature.as_string().unwrap());

            let start = zstd2.read_i32().unwrap();
            println!("  start: {}", start);

            let stop = zstd2.read_i32().unwrap();
            println!("  stop: {}", stop);

            let xstart = zstd2.read_i64().unwrap();
            println!("  xstart: {}", xstart);

            let xstop = zstd2.read_i64().unwrap();
            println!("  xstop: {}", xstop);

            // Check for separator
            let sep = zstd2.read_u8().unwrap();
            println!("  separator: 0x{:02x}", sep);
        }

        println!("\n✓ Successfully decoded exons array!");
    }
}

///! Debug test to see what's at the array position

use hail_decoder::buffer::{BlockingBuffer, InputBuffer, StreamBlockBuffer, ZstdBuffer};
use hail_decoder::codec::EncodedType;
use std::fs::File;

#[test]
fn test_debug_byte_position_before_array() {
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

    println!("\n=== Skipping to position before exons array ===\n");

    // Skip to after primitives
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

    // Skip known primitive fields
    string_type.read(&mut buffer).unwrap(); // gene_id
    string_type.read(&mut buffer).unwrap(); // gene_version
    string_type.read(&mut buffer).unwrap(); // symbol
    string_type.read(&mut buffer).unwrap(); // chrom
    string_type.read(&mut buffer).unwrap(); // strand
    buffer.read_i32().unwrap(); // start
    buffer.read_i32().unwrap(); // stop
    buffer.read_i64().unwrap(); // xstart
    buffer.read_i64().unwrap(); // xstop

    println!("After xstop field\n");

    // Now peek at the next 20 bytes
    let mut peek = [0u8; 20];
    buffer.read_exact(&mut peek).unwrap();

    println!("Next 20 bytes:");
    for (i, byte) in peek.iter().enumerate() {
        print!("{:02x} ", byte);
        if (i + 1) % 8 == 0 {
            println!();
        }
    }
    println!("\n");

    // Interpret different ways
    println!("Interpretations:");
    println!("  As bool: {} (0x{:02x})", peek[0] != 0, peek[0]);

    // Try reading as i32 (little-endian)
    let i32_val = i32::from_le_bytes([peek[0], peek[1], peek[2], peek[3]]);
    println!("  As i32: {}", i32_val);

    // Check if this looks like the "CDS" we saw before
    if peek.len() >= 6 {
        let possible_len = peek[5];
        if possible_len == 3 && peek.len() >= 9 {
            let possible_str = std::str::from_utf8(&peek[6..9]);
            println!("  If byte[5]=len and byte[6..9]=str: {:?}", possible_str);
        }
    }

    println!("\n=== Analysis ===");
    println!("If this is a nullable array:");
    println!("  - Byte 0 should be present flag (0x01 = present, 0x00 = null)");
    println!("  - Bytes 1-4 should be i32 length");
    println!("");
    println!("What we see:");
    println!("  - Byte 0 = 0x{:02x} ({})", peek[0], if peek[0] == 0 { "NULL" } else { "PRESENT" });
    println!("  - Bytes 1-4 as i32 = {}", i32_val);
}

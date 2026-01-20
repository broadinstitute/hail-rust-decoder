///! Test decoding arrays with proper format understanding

use hail_decoder::buffer::{InputBuffer, StreamBlockBuffer, ZstdBuffer};
use hail_decoder::codec::EncodedType;
use std::fs::File;

#[test]
fn test_array_header_exploration() {
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

    // Skip to array position (offset 0x0052)
    zstd.read_bool().unwrap();
    let mut skip = [0u8; 5];
    zstd.read_exact(&mut skip).unwrap();
    string_type.read(&mut zstd).unwrap();
    zstd.read_i32().unwrap();
    string_type.read(&mut zstd).unwrap();
    zstd.read_i32().unwrap();
    zstd.read_bool().unwrap();
    zstd.read_bool().unwrap();
    string_type.read(&mut zstd).unwrap();
    string_type.read(&mut zstd).unwrap();
    string_type.read(&mut zstd).unwrap();
    string_type.read(&mut zstd).unwrap();
    string_type.read(&mut zstd).unwrap();
    zstd.read_i32().unwrap();
    zstd.read_i32().unwrap();
    zstd.read_i64().unwrap();
    zstd.read_i64().unwrap();

    println!("\n=== Array Header Analysis ===");

    // Read potential header bytes
    let mut header = [0u8; 10];
    zstd.read_exact(&mut header).unwrap();
    println!("First 10 bytes: {:02x?}", header);

    // Try different interpretations
    println!("\nInterpretations:");
    println!("  Bytes 0-3 as i32: {}", i32::from_le_bytes([header[0], header[1], header[2], header[3]]));
    println!("  Bytes 1-4 as i32: {}", i32::from_le_bytes([header[1], header[2], header[3], header[4]]));
    println!("  Bytes 4-7 as i32: {}", i32::from_le_bytes([header[4], header[5], header[6], header[7]]));
    println!("  Bytes 5-8 as i32: {}", i32::from_le_bytes([header[5], header[6], header[7], header[8]]));

    println!("\n  Byte 5 as u8: {}", header[5]);
    println!("  Bytes 5-6 as u16: {}", u16::from_le_bytes([header[5], header[6]]));

    // The most likely: 5 bytes of something + i32 length
    // But header[5] = 0x03 suggests it might be: 5 zero bytes + 1 byte length
    // OR: it could be the array is at offset 5, so [4 zero bytes][i32 where first byte is 00]

    // Let's try: skip 5 bytes, then read i32
    // We've already read 10, so rewind
    let file2 = File::open(part_file.path()).unwrap();
    let stream2 = StreamBlockBuffer::new(file2);
    let mut zstd2 = ZstdBuffer::new(stream2);

    // Skip to array again
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

    println!("\n=== Theory: Skip 5 bytes, read i32 array length ===");
    let mut mystery_header = [0u8; 5];
    zstd2.read_exact(&mut mystery_header).unwrap();
    println!("Mystery header: {:02x?}", mystery_header);

    let array_length = zstd2.read_i32().unwrap();
    println!("Array length (i32): {}", array_length);

    // But wait, that would read [00 03 43 44] = 0x44430300 = 1145324544
    // That's wrong!

    println!("\n=== Theory: It's actually 4 zeros + byte length at position 5 ===");
    // Rewind again
    let file3 = File::open(part_file.path()).unwrap();
    let stream3 = StreamBlockBuffer::new(file3);
    let mut zstd3 = ZstdBuffer::new(stream3);

    zstd3.read_bool().unwrap();
    let mut skip3 = [0u8; 5];
    zstd3.read_exact(&mut skip3).unwrap();
    string_type.read(&mut zstd3).unwrap();
    zstd3.read_i32().unwrap();
    string_type.read(&mut zstd3).unwrap();
    zstd3.read_i32().unwrap();
    zstd3.read_bool().unwrap();
    zstd3.read_bool().unwrap();
    string_type.read(&mut zstd3).unwrap();
    string_type.read(&mut zstd3).unwrap();
    string_type.read(&mut zstd3).unwrap();
    string_type.read(&mut zstd3).unwrap();
    string_type.read(&mut zstd3).unwrap();
    zstd3.read_i32().unwrap();
    zstd3.read_i32().unwrap();
    zstd3.read_i64().unwrap();
    zstd3.read_i64().unwrap();

    // Read as: [4 bytes something][i32 array length]
    let prefix = zstd3.read_i32().unwrap();
    println!("Prefix i32: {} (0x{:08x})", prefix, prefix);

    let length = zstd3.read_i32().unwrap();
    println!("Array length: {} (0x{:08x})", length, length);

    // That reads [00 00 00 00] and [00 03 43 44]
    // Still wrong!

    println!("\n=== Conclusion: The format must be different ===");
    println!("Perhaps the 5 zeros are field presence bits for the struct?");
    println!("Or they're related to how nullable arrays are encoded?");
}

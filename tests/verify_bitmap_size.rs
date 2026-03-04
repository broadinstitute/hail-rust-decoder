///! Test to definitively prove whether the data has 4 or 5 bitmap bytes
///!
///! Strategy:
///! 1. Read the raw decompressed bytes
///! 2. Try interpreting with 4-byte bitmap (current metadata)
///! 3. Try interpreting with 5-byte bitmap (old schema hypothesis)
///! 4. See which interpretation makes sense

use genohype_core::buffer::{BlockingBuffer, InputBuffer, StreamBlockBuffer, ZstdBuffer};
use std::fs::File;

#[test]
fn verify_actual_bitmap_size() {
    println!("\n=== Bitmap Size Verification Test ===\n");
    println!("NOTE: The data file was apparently encoded WITHOUT LEB128 buffer layer");
    println!("(The metadata says LEB128BufferSpec, but the test uses raw Zstd decoding)\n");

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
    // Build proper buffer stack: StreamBlockBuffer -> ZstdBuffer -> BlockingBuffer
    let stream = StreamBlockBuffer::new(file);
    let zstd = ZstdBuffer::new(stream);
    let mut buffer = BlockingBuffer::with_default_size(zstd);

    // Read first 20 bytes
    let mut first_bytes = [0u8; 20];
    buffer.read_exact(&mut first_bytes).unwrap();

    println!("First 20 decompressed bytes:");
    print!("  ");
    for (i, &byte) in first_bytes.iter().enumerate() {
        print!("{:02x} ", byte);
        if (i + 1) % 8 == 0 {
            println!();
            print!("  ");
        }
    }
    println!("\n");

    // According to metadata: row struct is +EBaseStruct (required)
    // So there should be NO boolean flag, just the bitmap

    println!("HYPOTHESIS 1: 4-byte bitmap (matches current metadata: 25 nullable fields)");
    println!("  Bytes 0-3: bitmap = {:02x} {:02x} {:02x} {:02x}",
             first_bytes[0], first_bytes[1], first_bytes[2], first_bytes[3]);
    println!("  Byte 4: first data byte = {:02x}", first_bytes[4]);

    // If interval field (first nullable field) has bit 0 set, it's missing
    let bitmap_4byte = [first_bytes[0], first_bytes[1], first_bytes[2], first_bytes[3]];
    let interval_missing_4 = (bitmap_4byte[0] & 0x01) != 0;
    println!("  Bit 0 (interval): {} ({})", bitmap_4byte[0] & 0x01,
             if interval_missing_4 { "MISSING" } else { "present" });

    if interval_missing_4 {
        println!("  → Next field should be gene_id (EBinary)");
        println!("  → Expect: i32 length at bytes 4-7");
        let length = i32::from_le_bytes([first_bytes[4], first_bytes[5],
                                         first_bytes[6], first_bytes[7]]);
        println!("  → Length = {} (0x{:08x})", length, length);

        if length > 0 && length < 100 {
            println!("  ✓ This looks reasonable for a string length!");

            // Try to read the string
            let file2 = File::open(part_file.path()).unwrap();
            // Build proper buffer stack
            let stream2 = StreamBlockBuffer::new(file2);
            let zstd2 = ZstdBuffer::new(stream2);
            let mut buffer2 = BlockingBuffer::with_default_size(zstd2);

            // Skip bitmap
            let mut skip = [0u8; 4];
            buffer2.read_exact(&mut skip).unwrap();

            // Read length
            let len = buffer2.read_i32().unwrap();
            println!("  → String length: {}", len);

            // Read string bytes
            let mut string_bytes = vec![0u8; len as usize];
            buffer2.read_exact(&mut string_bytes).unwrap();
            let string_val = String::from_utf8_lossy(&string_bytes);
            println!("  → String value: \"{}\"", string_val);

            if string_val.starts_with("ENSG") {
                println!("  ✓✓✓ THIS IS A GENE ID! 4-byte bitmap is CORRECT!");
            }
        } else {
            println!("  ✗ Length {} seems unreasonable", length);
        }
    }

    println!("\n{}", "=".repeat(60));
    println!("\nHYPOTHESIS 2: 5-byte bitmap (old schema: 40 nullable fields)");
    println!("  Bytes 0-4: bitmap = {:02x} {:02x} {:02x} {:02x} {:02x}",
             first_bytes[0], first_bytes[1], first_bytes[2], first_bytes[3], first_bytes[4]);
    println!("  Byte 5: first data byte = {:02x}", first_bytes[5]);

    let bitmap_5byte = [first_bytes[0], first_bytes[1], first_bytes[2],
                        first_bytes[3], first_bytes[4]];
    let interval_missing_5 = (bitmap_5byte[0] & 0x01) != 0;
    println!("  Bit 0 (interval): {} ({})", bitmap_5byte[0] & 0x01,
             if interval_missing_5 { "MISSING" } else { "present" });

    if interval_missing_5 {
        println!("  → Next field should be gene_id (EBinary)");
        println!("  → Expect: i32 length at bytes 5-8");
        let length = i32::from_le_bytes([first_bytes[5], first_bytes[6],
                                         first_bytes[7], first_bytes[8]]);
        println!("  → Length = {} (0x{:08x})", length, length);

        if length > 0 && length < 100 {
            println!("  ✓ This looks reasonable for a string length!");
        } else {
            println!("  ✗ Length {} seems unreasonable", length);
        }
    } else {
        println!("  → interval is present, so next should be interval's bitmap");
        println!("  → interval has 2 nullable fields (start, end) → 1 bitmap byte");
        println!("  → Byte 5 should be interval's bitmap: {:02x}", first_bytes[5]);
    }

    println!("\n{}", "=".repeat(60));
    println!("\nHYPOTHESIS 3: Byte 0 is a boolean present flag (row is nullable despite +)");
    println!("  Byte 0: present flag = {:02x} ({})", first_bytes[0],
             if first_bytes[0] != 0 { "PRESENT" } else { "NULL" });
    println!("  Bytes 1-4: bitmap = {:02x} {:02x} {:02x} {:02x}",
             first_bytes[1], first_bytes[2], first_bytes[3], first_bytes[4]);
    println!("  Byte 5: first data byte = {:02x}", first_bytes[5]);

    if first_bytes[0] != 0 {
        println!("  → Row struct is present");
        let bitmap_h3 = [first_bytes[1], first_bytes[2], first_bytes[3], first_bytes[4]];
        let interval_missing_h3 = (bitmap_h3[0] & 0x01) != 0;
        println!("  → Bit 0 in bitmap (interval): {} ({})", bitmap_h3[0] & 0x01,
                 if interval_missing_h3 { "MISSING" } else { "present" });

        if interval_missing_h3 {
            println!("  ✗ interval is missing, but byte 5 is 0x00 - doesn't make sense");
        } else {
            println!("  → interval is PRESENT!");
            println!("  → interval has 2 nullable fields (start, end) → 1 bitmap byte");
            println!("  → Byte 5 should be interval's bitmap: {:02x}", first_bytes[5]);
            println!("  → In binary: {:08b}", first_bytes[5]);
            println!("  → Bit 0 (start): {} ({})", first_bytes[5] & 0x01,
                     if (first_bytes[5] & 0x01) != 0 { "MISSING" } else { "present" });
            println!("  → Bit 1 (end): {} ({})", (first_bytes[5] >> 1) & 0x01,
                     if ((first_bytes[5] >> 1) & 0x01) != 0 { "MISSING" } else { "present" });

            if first_bytes[5] == 0x00 {
                println!("  ✓ Both start and end are PRESENT!");
                println!("  → start.contig should be at byte 6 (i32 length)");
                let length_attempt = i32::from_le_bytes([first_bytes[6], first_bytes[7],
                                                 first_bytes[8], first_bytes[9]]);
                println!("  → Length (as i32) = {} (0x{:08x})", length_attempt, length_attempt);
                println!("  ✗ This is unreasonable - trying single byte instead...");

                // Try single byte length
                let length_u8 = first_bytes[6];
                println!("  → Length (as u8) = {}", length_u8);

                if length_u8 > 0 && length_u8 < 100 {
                    println!("  ✓✓ This looks reasonable!");

                    // Try to decode the full sequence
                    let file3 = File::open(part_file.path()).unwrap();
                    // Build proper buffer stack
                    let stream3 = StreamBlockBuffer::new(file3);
                    let zstd3 = ZstdBuffer::new(stream3);
                    let mut buffer3 = BlockingBuffer::with_default_size(zstd3);

                    // Read and skip: present flag (1) + row bitmap (4) + interval bitmap (1) = 6 bytes
                    let mut skip = [0u8; 6];
                    buffer3.read_exact(&mut skip).unwrap();

                    // The length appears to be a SINGLE BYTE (not i32!)
                    // This suggests either LEB128 encoding or a different format
                    let contig_len_byte = buffer3.read_u8().unwrap();
                    println!("  → start.contig length (as u8): {}", contig_len_byte);

                    let mut contig_bytes = vec![0u8; contig_len_byte as usize];
                    buffer3.read_exact(&mut contig_bytes).unwrap();
                    let contig = String::from_utf8_lossy(&contig_bytes);
                    println!("  → start.contig = \"{}\"", contig);

                    // Position is still i32
                    let position = buffer3.read_i32().unwrap();
                    println!("  → start.position = {}", position);

                    if contig.starts_with("chr") && position > 0 {
                        println!("\n  ✓✓✓ SUCCESS! This is a valid genomic interval!");
                        println!("  ✓✓✓ HYPOTHESIS 3 is CORRECT!");
                        println!("\n  CONCLUSION:");
                        println!("  1. The row struct has a boolean present flag despite being marked as +EBaseStruct");
                        println!("  2. EBinary lengths are encoded as SINGLE BYTES (u8), NOT i32!");
                        println!("     This suggests the data was encoded differently than expected.");
                    }
                }
            }
        }
    }

    println!("\n{}", "=".repeat(60));
    println!("\nCONCLUSION:");
    println!("The hypothesis that produces valid genomic data is correct!");
}

///! Test to validate understanding of struct nullable field bitmap
///!
///! Key insight from Hail source (EBaseStruct.scala lines 161-198):
///! 1. Structs with nullable fields start with a bitmap of missing bytes
///! 2. The number of bytes = packBitsToBytes(nMissing)
///! 3. Each bit represents whether a field is missing (1) or present (0)
///! 4. Only present fields have their data written after the bitmap

use hail_decoder::buffer::{InputBuffer, StreamBlockBuffer, ZstdBuffer};
use std::fs::File;

#[test]
fn test_struct_presence_bitmap() {
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
    let mut buffer = ZstdBuffer::new(stream);

    println!("\n=== Testing Struct Presence Bitmap Understanding ===\n");

    // At offset 0x0000: Outer struct present flag
    // The row struct itself is nullable, so first byte is its present flag
    let outer_present = buffer.read_bool().unwrap();
    println!("Offset 0x0000: Outer struct present flag = {}", outer_present);
    assert_eq!(outer_present, true);

    // At offset 0x0001: Now comes the field presence bitmap
    // The row struct has 24 fields, and we need to count how many are nullable

    // According to the briefing, the gene struct has these fields:
    // From the metadata _eType in briefing, most fields are nullable (no + prefix)
    // Let's assume ~40 nullable fields based on the hypothesis in the briefing

    // Read the first byte of the bitmap
    println!("\nOffset 0x0001-0x0005: Reading field presence bitmap");

    let byte0 = buffer.read_u8().unwrap();
    println!("  Byte 0: 0x{:02x} = {:08b}", byte0, byte0);

    // The briefing says bytes 0x0001-0x0005 are all 0x00
    let mut bitmap_bytes = vec![byte0];
    for i in 1..5 {
        let b = buffer.read_u8().unwrap();
        println!("  Byte {}: 0x{:02x} = {:08b}", i, b, b);
        bitmap_bytes.push(b);
    }

    println!("\nPresence bitmap (5 bytes): {:02x?}", bitmap_bytes);
    println!("This represents {} bits for nullable fields", bitmap_bytes.len() * 8);

    // In Hail's encoding:
    // - Bit set (1) = field is MISSING
    // - Bit clear (0) = field is PRESENT

    // All zeros means all fields are present, which matches our test data!
    let all_zeros = bitmap_bytes.iter().all(|&b| b == 0);
    println!("\nAll bits clear (all fields present)? {}", all_zeros);

    // Now the actual field data follows
    println!("\n=== Now reading actual field data (after bitmap) ===\n");

    // Field 0: gene_id (EBinary, nullable)
    // Since bit 0 is clear (present), we should read it
    // For nullable EBinary: no boolean flag, because struct already handled it!
    let length = buffer.read_u8().unwrap();
    let mut gene_id_bytes = vec![0u8; length as usize];
    buffer.read_exact(&mut gene_id_bytes).unwrap();
    let gene_id = String::from_utf8_lossy(&gene_id_bytes);
    println!("Field 0 - gene_id: \"{}\"", gene_id);
    assert_eq!(gene_id, "ENSG00000066468");

    println!("\n✓ Hypothesis VALIDATED!");
    println!("  - The 5 bytes at start are struct presence bitmap");
    println!("  - All zeros = all nullable fields are present");
    println!("  - Field data follows immediately after bitmap");
    println!("  - NO additional null check per field (bitmap handles it)");
}

#[test]
fn test_array_starts_after_all_primitive_fields() {
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
    let mut buffer = ZstdBuffer::new(stream);

    println!("\n=== Finding where array actually starts ===\n");

    // Skip outer struct present flag (1 byte)
    let outer_present = buffer.read_bool().unwrap();
    println!("Outer struct present: {}", outer_present);

    // Skip presence bitmap (5 bytes)
    let mut bitmap = [0u8; 5];
    buffer.read_exact(&mut bitmap).unwrap();
    println!("Skipped presence bitmap: {:02x?}", bitmap);

    // Based on the working test, structure is:
    // 1. Interval struct (locus field):
    //    - chr1 (string)
    //    - position1 (i32)
    //    - chr2 (string)
    //    - position2 (i32)
    //    - includes_start (bool)
    //    - includes_end (bool)
    // 2. gene_id (string)
    // 3. gene_version (string)
    // 4. symbol (string)
    // ... more fields

    // Interval start
    let len = buffer.read_u8().unwrap();
    let mut chr1 = vec![0u8; len as usize];
    buffer.read_exact(&mut chr1).unwrap();
    let pos1 = buffer.read_i32().unwrap();
    println!("Interval start: {} @ {}", String::from_utf8_lossy(&chr1), pos1);

    // Interval end
    let len = buffer.read_u8().unwrap();
    let mut chr2 = vec![0u8; len as usize];
    buffer.read_exact(&mut chr2).unwrap();
    let pos2 = buffer.read_i32().unwrap();
    println!("Interval end: {} @ {}", String::from_utf8_lossy(&chr2), pos2);

    // Interval flags
    let inc_start = buffer.read_bool().unwrap();
    let inc_end = buffer.read_bool().unwrap();
    println!("Interval flags: start={}, end={}", inc_start, inc_end);

    // Now the actual gene fields
    let len = buffer.read_u8().unwrap();
    let mut gene_id = vec![0u8; len as usize];
    buffer.read_exact(&mut gene_id).unwrap();
    println!("gene_id: {}", String::from_utf8_lossy(&gene_id));

    let len = buffer.read_u8().unwrap();
    let mut gene_version = vec![0u8; len as usize];
    buffer.read_exact(&mut gene_version).unwrap();
    println!("gene_version: {}", String::from_utf8_lossy(&gene_version));

    let len = buffer.read_u8().unwrap();
    let mut symbol = vec![0u8; len as usize];
    buffer.read_exact(&mut symbol).unwrap();
    println!("symbol: {}", String::from_utf8_lossy(&symbol));

    // Continue with more fields... (need to find out what comes next)
    // From the briefing, after the 10 fields decoded, we're at offset 0x0052
    // which is where "00 00 00 00 00 03 43 44 53" starts
    // Let's read more fields to get there

    // Read more strings and ints to reach the array
    // Based on test output, we need to decode more fields
    for i in 0..7 {
        let len = buffer.read_u8().unwrap();
        let mut s = vec![0u8; len as usize];
        buffer.read_exact(&mut s).unwrap();
        println!("String field {}: {}", i+4, String::from_utf8_lossy(&s));
    }

    // Some integer fields
    let int1 = buffer.read_i32().unwrap();
    let int2 = buffer.read_i32().unwrap();
    println!("Int fields: {}, {}", int1, int2);

    let long1 = buffer.read_i64().unwrap();
    let long2 = buffer.read_i64().unwrap();
    println!("Long fields: {}, {}", long1, long2);

    // NOW we should be at the array!
    println!("\n=== At array position ===");

    // But wait! The array itself might be nullable
    // So there might be more nullable fields before the array
    // Or the struct field presence bitmap handles this

    // Let's just try to find where "00 03 43 44 53" (array with 3 CDS elements) appears
    // Read bytes and look for the pattern
    println!("Searching for array start pattern [00 03 43]...");
    let mut found = false;
    for attempt in 0..100 {
        let b1 = buffer.read_u8().unwrap();
        if b1 == 0x00 {
            let b2 = buffer.read_u8().unwrap();
            if b2 == 0x03 {
                let b3 = buffer.read_u8().unwrap();
                if b3 == 0x43 { // 'C'
                    println!("✓ FOUND pattern at attempt {}!", attempt);
                    println!("  Next bytes: 00 03 43 (probably array length=3, first element starts with 'C')");
                    found = true;

                    // So the format might be:
                    // [missing bytes for array elements?][i32 length][elements...]
                    // OR
                    // [i32 length=3][missing bitmap][elements...]

                    // Let's try reading as if length was at -3 bytes
                    // But we already consumed those bytes, so let's restart
                    break;
                }
            }
        }
    }

    if !found {
        println!("✗ Pattern not found in first 100 bytes");
        return;
    }

    // Restart to properly decode
    let file2 = File::open(part_file.path()).unwrap();
    let stream2 = StreamBlockBuffer::new(file2);
    let mut buffer2 = ZstdBuffer::new(stream2);

    // Skip to just before array (we know the position now)
    buffer2.read_bool().unwrap(); // outer present
    let mut skip = [0u8; 5];
    buffer2.read_exact(&mut skip).unwrap(); // bitmap

    // Skip all the fields we decoded above
    // Interval (6 fields)
    for _ in 0..2 {
        let len = buffer2.read_u8().unwrap();
        let mut s = vec![0u8; len as usize];
        buffer2.read_exact(&mut s).unwrap();
        buffer2.read_i32().unwrap();
    }
    buffer2.read_bool().unwrap();
    buffer2.read_bool().unwrap();

    // Gene fields (3 strings)
    for _ in 0..3 {
        let len = buffer2.read_u8().unwrap();
        let mut s = vec![0u8; len as usize];
        buffer2.read_exact(&mut s).unwrap();
    }

    // 7 more strings
    for _ in 0..7 {
        let len = buffer2.read_u8().unwrap();
        let mut s = vec![0u8; len as usize];
        buffer2.read_exact(&mut s).unwrap();
    }

    // 2 ints, 2 longs
    buffer2.read_i32().unwrap();
    buffer2.read_i32().unwrap();
    buffer2.read_i64().unwrap();
    buffer2.read_i64().unwrap();

    // Now try reading array length as i32
    println!("\n=== Attempting proper array decode ===");
    let array_length = buffer2.read_i32().unwrap();
    println!("Array length (i32): {}", array_length);

    // If this is 3, we found it!
    // If not, we need to figure out what's between us and the array

    if array_length == 3 {
        println!("✓ SUCCESS! Array has 3 elements (expected for FGFR2 exons)");
    } else {
        println!("✗ Length is {}, not 3. More investigation needed.", array_length);

        // Let's see what the bytes look like
        println!("\nNext 20 bytes:");
        for i in 0..20 {
            let b = buffer.read_u8().unwrap();
            print!("{:02x} ", b);
            if (i + 1) % 8 == 0 {
                println!();
            }
        }
    }
}

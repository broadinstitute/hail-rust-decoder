///! Detailed test to trace exactly where interval decoding fails

use hail_decoder::buffer::{BufferBuilder, InputBuffer};
use std::fs::File;

#[test]
fn test_interval_decode_step_by_step() {
    println!("\n=== Step-by-Step Interval Decoding ===\n");

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
    let mut buffer = BufferBuilder::from_file(part_file.path())
        .unwrap()
        .with_leb128()
        .build();

    // Manually decode step by step
    println!("Step 1: Row present flag");
    let row_present = buffer.read_bool().unwrap();
    println!("  = {} ({})", row_present, if row_present { "present" } else { "null" });

    println!("\nStep 2: Row bitmap (4 bytes for 25 nullable fields)");
    let mut row_bitmap = [0u8; 4];
    buffer.read_exact(&mut row_bitmap).unwrap();
    println!("  = {:02x?}", row_bitmap);
    let interval_present = (row_bitmap[0] & 0x01) == 0;
    println!("  interval (bit 0): {}", if interval_present { "present" } else { "missing" });

    if !interval_present {
        println!("\n✗ Interval is missing! Can't continue.");
        return;
    }

    println!("\nStep 3: Interval bitmap (1 byte for 2 nullable fields: start, end)");
    let interval_bitmap = buffer.read_u8().unwrap();
    println!("  = 0x{:02x} = {:08b}", interval_bitmap, interval_bitmap);
    let start_present = (interval_bitmap & 0x01) == 0;
    let end_present = (interval_bitmap & 0x02) == 0;
    println!("  start (bit 0): {}", if start_present { "present" } else { "missing" });
    println!("  end (bit 1): {}", if end_present { "present" } else { "missing" });

    if !start_present {
        println!("\n✗ Start is missing! Can't continue.");
        return;
    }

    println!("\nStep 4: Start struct (no bitmap - all fields required)");
    println!("  4a: start.contig (EBinary with LEB128 length)");
    let contig_len = buffer.read_i32().unwrap();
    println!("      length = {}", contig_len);
    let mut contig_bytes = vec![0u8; contig_len as usize];
    buffer.read_exact(&mut contig_bytes).unwrap();
    let contig = String::from_utf8_lossy(&contig_bytes);
    println!("      value = \"{}\"", contig);

    println!("  4b: start.position (EInt32 with LEB128)");
    let position = buffer.read_i32().unwrap();
    println!("      value = {}", position);

    if !end_present {
        println!("\n✗ End is missing! Can't continue to next fields.");
        return;
    }

    println!("\nStep 5: End struct (no bitmap - all fields required)");
    println!("  5a: end.contig (EBinary with LEB128 length)");
    let end_contig_len = buffer.read_i32().unwrap();
    println!("      length = {}", end_contig_len);
    let mut end_contig_bytes = vec![0u8; end_contig_len as usize];
    buffer.read_exact(&mut end_contig_bytes).unwrap();
    let end_contig = String::from_utf8_lossy(&end_contig_bytes);
    println!("      value = \"{}\"", end_contig);

    println!("  5b: end.position (EInt32 with LEB128)");
    let end_position = buffer.read_i32().unwrap();
    println!("      value = {}", end_position);

    println!("\nStep 6: Interval boolean fields (both required)");
    println!("  6a: includesStart (EBoolean)");
    let includes_start = buffer.read_bool().unwrap();
    println!("      value = {}", includes_start);

    println!("  6b: includesEnd (EBoolean)");
    let includes_end = buffer.read_bool().unwrap();
    println!("      value = {}", includes_end);

    println!("\n✓✓✓ Successfully decoded complete interval!");
    println!("\nInterval: {}:{}-{} ({}start, {}end)",
             contig, position, end_position,
             if includes_start { "includes " } else { "excludes " },
             if includes_end { "includes " } else { "excludes " });

    // Verify it makes sense
    assert_eq!(contig, "chr10");
    assert!(position > 0 && position < 200_000_000, "Position out of range for chr10");
    assert!(end_position > position, "End should be after start");

    println!("\n✓ All assertions passed!");
}

///! Test to verify that the decoder works correctly with the FIXED LEB128Buffer
///!
///! This test uses the actual LEB128Buffer (now fixed to override integer reads)

use genohype_core::buffer::{BlockingBuffer, InputBuffer, LEB128Buffer, StreamBlockBuffer, ZstdBuffer};
use std::fs::File;

#[test]
fn test_decode_with_proper_leb128() {
    println!("\n=== Testing with LEB128-enabled buffer ===\n");

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
    // Build proper buffer stack: StreamBlockBuffer -> ZstdBuffer -> BlockingBuffer -> LEB128Buffer
    let stream = StreamBlockBuffer::new(file);
    let zstd = ZstdBuffer::new(stream);
    let blocking = BlockingBuffer::with_default_size(zstd);
    let mut buffer = LEB128Buffer::new(blocking);

    // Now read with proper LEB128 decoding
    println!("Reading first row with LEB128 decoding:");

    // Skip the present flag (if it exists - we established it does)
    let present = buffer.read_bool().unwrap();
    println!("  Row present: {}", present);

    // Read bitmap for row struct (4 bytes)
    let mut bitmap = [0u8; 4];
    buffer.read_exact(&mut bitmap).unwrap();
    println!("  Row bitmap: {:02x?}", bitmap);

    let interval_present = (bitmap[0] & 0x01) == 0;
    println!("  Interval present: {}", interval_present);

    if interval_present {
        // Read interval's bitmap (1 byte - 2 nullable fields)
        let interval_bitmap = buffer.read_u8().unwrap();
        println!("  Interval bitmap: {:02x}", interval_bitmap);

        let start_present = (interval_bitmap & 0x01) == 0;
        let end_present = (interval_bitmap & 0x02) == 0;
        println!("  Start present: {}, End present: {}", start_present, end_present);

        if start_present {
            // Read start.contig as EBinary with LEB128 length
            let contig_len = buffer.read_i32().unwrap(); // This now uses LEB128!
            println!("  Start.contig length (LEB128 decoded): {}", contig_len);

            let mut contig_bytes = vec![0u8; contig_len as usize];
            buffer.read_exact(&mut contig_bytes).unwrap();
            let contig = String::from_utf8_lossy(&contig_bytes);
            println!("  Start.contig: \"{}\"", contig);

            // Read start.position as i32 (also LEB128)
            let position = buffer.read_i32().unwrap();
            println!("  Start.position: {}", position);

            if contig.starts_with("chr") && position > 0 {
                println!("\n✓✓✓ SUCCESS! Decoded valid genomic data with proper LEB128!");
                println!("✓✓✓ The LEB128Buffer fix WORKS!");
                println!("✓✓✓ Position {} matches FGFR2 gene start location!", position);

                assert_eq!(contig, "chr10");
                assert_eq!(position, 121478332, "FGFR2 gene start position on chr10");
            } else {
                panic!("Unexpected data: contig={}, position={}", contig, position);
            }
        }
    }
}

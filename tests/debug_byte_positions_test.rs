///! Debug test to carefully track byte positions

use hail_decoder::buffer::{InputBuffer, StreamBlockBuffer, ZstdBuffer};
use hail_decoder::codec::EncodedType;
use std::fs::File;

#[test]
fn test_trace_byte_by_byte() {
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

    let mut offset = 0;

    // Helper to track reads
    macro_rules! read_and_track {
        ($read_expr:expr, $size:expr, $desc:expr) => {{
            let result = $read_expr;
            println!("[{:04x}] {} = {:?}", offset, $desc, result);
            offset += $size;
            result
        }};
    }

    let string_type = EncodedType::EBinary { required: true };

    // Read and track each field
    read_and_track!(zstd.read_bool().unwrap(), 1, "outer_present");

    let mut mystery = [0u8; 5];
    zstd.read_exact(&mut mystery).unwrap();
    println!("[{:04x}] mystery = {:02x?}", offset, mystery);
    offset += 5;

    // Interval
    println!("\n=== Interval ===");
    let chr1 = string_type.read(&mut zstd).unwrap();
    let chr1_str = chr1.as_string().unwrap();
    println!("[{:04x}] chr_start (len={}) = {:?}", offset, chr1_str.len() + 1, chr1_str);
    offset += chr1_str.len() + 1;

    read_and_track!(zstd.read_i32().unwrap(), 4, "pos_start");

    let chr2 = string_type.read(&mut zstd).unwrap();
    let chr2_str = chr2.as_string().unwrap();
    println!("[{:04x}] chr_end (len={}) = {:?}", offset, chr2_str.len() + 1, chr2_str);
    offset += chr2_str.len() + 1;

    read_and_track!(zstd.read_i32().unwrap(), 4, "pos_end");
    read_and_track!(zstd.read_bool().unwrap(), 1, "inc_start");
    read_and_track!(zstd.read_bool().unwrap(), 1, "inc_end");

    // Gene fields
    println!("\n=== Gene Fields ===");
    let gene_id = string_type.read(&mut zstd).unwrap();
    let gene_id_str = gene_id.as_string().unwrap();
    println!("[{:04x}] gene_id (len={}) = {:?}", offset, gene_id_str.len() + 1, gene_id_str);
    offset += gene_id_str.len() + 1;

    let gene_ver = string_type.read(&mut zstd).unwrap();
    let gene_ver_str = gene_ver.as_string().unwrap();
    println!("[{:04x}] gene_version (len={}) = {:?}", offset, gene_ver_str.len() + 1, gene_ver_str);
    offset += gene_ver_str.len() + 1;

    let symbol = string_type.read(&mut zstd).unwrap();
    let symbol_str = symbol.as_string().unwrap();
    println!("[{:04x}] symbol (len={}) = {:?}", offset, symbol_str.len() + 1, symbol_str);
    offset += symbol_str.len() + 1;

    let chrom = string_type.read(&mut zstd).unwrap();
    let chrom_str = chrom.as_string().unwrap();
    println!("[{:04x}] chrom (len={}) = {:?}", offset, chrom_str.len() + 1, chrom_str);
    offset += chrom_str.len() + 1;

    let strand = string_type.read(&mut zstd).unwrap();
    let strand_str = strand.as_string().unwrap();
    println!("[{:04x}] strand (len={}) = {:?}", offset, strand_str.len() + 1, strand_str);
    offset += strand_str.len() + 1;

    println!("\n=== Numeric Fields ===");
    read_and_track!(zstd.read_i32().unwrap(), 4, "start (i32)");
    read_and_track!(zstd.read_i32().unwrap(), 4, "stop (i32)");

    // Read xstart carefully
    let xstart = zstd.read_i64().unwrap();
    println!("[{:04x}] xstart (i64) = {} (0x{:016x})", offset, xstart, xstart);
    offset += 8;

    let xstop = zstd.read_i64().unwrap();
    println!("[{:04x}] xstop (i64) = {} (0x{:016x})", offset, xstop, xstop);
    offset += 8;

    // Now read the next 16 bytes to see what's coming
    println!("\n=== Next 16 bytes ===");
    let mut next_bytes = [0u8; 16];
    zstd.read_exact(&mut next_bytes).unwrap();
    println!("[{:04x}] next = {:02x?}", offset, next_bytes);

    // Try to interpret as array length
    let array_len = i32::from_le_bytes([next_bytes[0], next_bytes[1], next_bytes[2], next_bytes[3]]);
    println!("  Interpreted as i32 array length: {}", array_len);
}

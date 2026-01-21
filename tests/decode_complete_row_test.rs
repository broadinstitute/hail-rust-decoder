///! Test decoding a complete row with all primitive fields

use hail_decoder::buffer::{BlockingBuffer, InputBuffer, StreamBlockBuffer, ZstdBuffer};
use hail_decoder::codec::{EncodedType, EncodedValue};
use std::fs::File;

#[test]
fn test_decode_all_primitive_fields() {
    // Decode all primitive fields from the first gene (FGFR2)
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

    println!("\n=== Decoding FGFR2 Gene ===\n");

    // Outer struct present flag
    let outer_present = buffer.read_bool().unwrap();
    assert_eq!(outer_present, true);
    println!("Outer struct present: {}", outer_present);

    // Mystery bytes (5 bytes)
    let mut mystery = [0u8; 5];
    buffer.read_exact(&mut mystery).unwrap();
    println!("Mystery bytes: {:02x?}", mystery);

    let string_type = EncodedType::EBinary { required: true };

    // === INTERVAL ===
    println!("\n--- Interval ---");
    let chr_start = string_type.read(&mut buffer).unwrap();
    let pos_start = buffer.read_i32().unwrap();
    let chr_end = string_type.read(&mut buffer).unwrap();
    let pos_end = buffer.read_i32().unwrap();
    let inc_start = buffer.read_bool().unwrap();
    let inc_end = buffer.read_bool().unwrap();

    println!("  Start: {}:{}", chr_start.as_string().unwrap(), pos_start);
    println!("  End: {}:{}", chr_end.as_string().unwrap(), pos_end);
    println!("  Includes: start={}, end={}", inc_start, inc_end);

    assert_eq!(chr_start.as_string(), Some("chr10".to_string()));
    assert_eq!(chr_end.as_string(), Some("chr10".to_string()));
    assert_eq!(inc_start, true);
    assert_eq!(inc_end, true);

    // === GENE_ID ===
    let gene_id = string_type.read(&mut buffer).unwrap();
    println!("\nGene ID: {}", gene_id.as_string().unwrap());
    assert_eq!(gene_id.as_string(), Some("ENSG00000066468".to_string()));

    // === GENE_VERSION ===
    let gene_version = string_type.read(&mut buffer).unwrap();
    println!("Gene Version: {}", gene_version.as_string().unwrap());
    assert_eq!(gene_version.as_string(), Some("24".to_string()));

    // === GENCODE_SYMBOL (Symbol) ===
    let symbol = string_type.read(&mut buffer).unwrap();
    println!("Symbol: {}", symbol.as_string().unwrap());
    assert_eq!(symbol.as_string(), Some("FGFR2".to_string()));

    // === CHROM ===
    let chrom = string_type.read(&mut buffer).unwrap();
    println!("Chrom: {}", chrom.as_string().unwrap());
    assert_eq!(chrom.as_string(), Some("10".to_string()));

    // === STRAND ===
    let strand = string_type.read(&mut buffer).unwrap();
    println!("Strand: {}", strand.as_string().unwrap());
    assert_eq!(strand.as_string(), Some("-".to_string()));

    // === START (Int32) ===
    let start = buffer.read_i32().unwrap();
    println!("Start: {}", start);

    // === STOP (Int32) ===
    let stop = buffer.read_i32().unwrap();
    println!("Stop: {}", stop);

    // === XSTART (Int64) ===
    let xstart = buffer.read_i64().unwrap();
    println!("XStart: {}", xstart);

    // === XSTOP (Int64) ===
    let xstop = buffer.read_i64().unwrap();
    println!("XStop: {}", xstop);

    println!("\n=== Summary ===");
    println!("Gene: {} ({})", symbol.as_string().unwrap(), gene_id.as_string().unwrap());
    println!("Location: chr{}:{}-{} ({})",
        chrom.as_string().unwrap(), start, stop, strand.as_string().unwrap());
    println!("Extended: {}-{}", xstart, xstop);

    println!("\n✓ Successfully decoded all primitive fields!");
}

#[test]
fn test_decode_with_array_inspection() {
    // After primitive fields, we should encounter the exons array
    // Let's peek at what comes next
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

    // Skip to after primitive fields
    // Outer present + mystery (6 bytes)
    let mut skip = [0u8; 6];
    buffer.read_exact(&mut skip).unwrap();

    let string_type = EncodedType::EBinary { required: true };

    // Skip interval (2 strings + 2 i32s + 2 bools)
    string_type.read(&mut buffer).unwrap(); // chr start
    buffer.read_i32().unwrap(); // pos start
    string_type.read(&mut buffer).unwrap(); // chr end
    buffer.read_i32().unwrap(); // pos end
    buffer.read_bool().unwrap(); // inc start
    buffer.read_bool().unwrap(); // inc end

    // Skip gene fields
    string_type.read(&mut buffer).unwrap(); // gene_id
    string_type.read(&mut buffer).unwrap(); // gene_version
    string_type.read(&mut buffer).unwrap(); // symbol
    string_type.read(&mut buffer).unwrap(); // chrom
    string_type.read(&mut buffer).unwrap(); // strand

    // Skip numeric fields
    buffer.read_i32().unwrap(); // start
    buffer.read_i32().unwrap(); // stop
    buffer.read_i64().unwrap(); // xstart
    buffer.read_i64().unwrap(); // xstop

    // Now we should be at the exons array
    println!("\n=== Inspecting Array Data ===");

    // Arrays typically start with a length (i32)
    let array_length = buffer.read_i32().unwrap();
    println!("Array length: {}", array_length);

    if array_length > 0 && array_length < 1000 {
        println!("\nFirst array element:");

        // Each exon is a struct with: feature_type, start, stop, xstart, xstop
        // Let's try to read the first one
        let feature_type = string_type.read(&mut buffer).unwrap();
        println!("  feature_type: {}", feature_type.as_string().unwrap());

        let exon_start = buffer.read_i32().unwrap();
        println!("  start: {}", exon_start);

        let exon_stop = buffer.read_i32().unwrap();
        println!("  stop: {}", exon_stop);

        let exon_xstart = buffer.read_i64().unwrap();
        println!("  xstart: {}", exon_xstart);

        let exon_xstop = buffer.read_i64().unwrap();
        println!("  xstop: {}", exon_xstop);

        println!("\n✓ Successfully decoded first array element!");
    }
}

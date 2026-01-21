//! Hail decoder CLI tool
//!
//! Commands:
//! - info: Show basic table metadata
//! - inspect: Show detailed table information including keys and index status
//! - query: Query a table with optional key filters
//! - summary: Show comprehensive table summary with statistics

use hail_decoder::codec::EncodedValue;
use hail_decoder::io::{get_file_size, join_path};
use hail_decoder::metadata::RVDComponentSpec;
use hail_decoder::query::{KeyRange, KeyValue, QueryEngine};
use hail_decoder::summary::{format_schema_clean, StatsAccumulator};
use hail_decoder::validation::{SchemaGenerator, SchemaValidator};
use hail_decoder::Result;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage(&args[0]);
        std::process::exit(1);
    }

    match args[1].as_str() {
        "info" => {
            if args.len() < 3 {
                eprintln!("Usage: {} info <table.ht>", args[0]);
                std::process::exit(1);
            }
            show_info(&args[2])?;
        }
        "inspect" => {
            if args.len() < 3 {
                eprintln!("Usage: {} inspect <table.ht>", args[0]);
                std::process::exit(1);
            }
            inspect_table(&args[2])?;
        }
        "summary" => {
            if args.len() < 3 {
                eprintln!("Usage: {} summary <table.ht>", args[0]);
                std::process::exit(1);
            }
            run_summary(&args[2])?;
        }
        "query" => {
            run_query(&args[0], &args[2..])?;
        }
        "convert" => {
            if args.len() < 4 {
                eprintln!("Usage: {} convert <table.ht> <output.parquet>", args[0]);
                std::process::exit(1);
            }
            convert(&args[2], &args[3])?;
        }
        "validate" => {
            run_validate(&args[0], &args[2..])?;
        }
        "generate-schema" => {
            run_generate_schema(&args[0], &args[2..])?;
        }
        "help" | "--help" | "-h" => {
            print_usage(&args[0]);
        }
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            print_usage(&args[0]);
            std::process::exit(1);
        }
    }

    Ok(())
}

fn print_usage(program: &str) {
    eprintln!("Hail Decoder - Read and query Hail tables");
    eprintln!();
    eprintln!("Usage: {} <command> [args...]", program);
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  info <table.ht>              Show basic table metadata");
    eprintln!("  inspect <table.ht>           Show detailed table information");
    eprintln!("  summary <table.ht>           Show comprehensive table summary with statistics");
    eprintln!("  query <table.ht> [options]   Query the table");
    eprintln!("  validate <table.ht> <schema.json> [options]  Validate table against JSON schema");
    eprintln!("  generate-schema <table.ht> [output.json]    Generate JSON schema from table");
    eprintln!("  convert <table.ht> <out>     Convert to Parquet format");
    eprintln!();
    eprintln!("Query options:");
    eprintln!("  --key <field=value>          Point lookup (exact match)");
    eprintln!("  --where <field=value>        Filter condition (equality)");
    eprintln!("  --where <field>gt<value>     Filter: field > value");
    eprintln!("  --where <field>lt<value>     Filter: field < value");
    eprintln!("  --where <field>gte<value>    Filter: field >= value");
    eprintln!("  --where <field>lte<value>    Filter: field <= value");
    eprintln!("  --limit <n>                  Limit number of results");
    eprintln!("  --json                       Output as JSON");
    eprintln!();
    eprintln!("Validate options:");
    eprintln!("  --limit <n>                  Validate first N rows (sequential)");
    eprintln!("  --sample <n>                 Validate N randomly sampled rows across partitions");
    eprintln!("  --fail-fast                  Stop on first validation error");
    eprintln!();
    eprintln!("Nested fields:");
    eprintln!("  Use dot notation for nested struct fields (e.g., locus.contig)");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  {} inspect gene_models.ht", program);
    eprintln!("  {} query gene_models.ht --key gene_id=ENSG00000141510", program);
    eprintln!("  {} query gene_models.ht --where chrom=chr1 --limit 10", program);
    eprintln!("  {} query variants.ht --where locus.contig=chr1 --where \"locus.position>=100000\" --limit 10", program);
    eprintln!("  {} validate gene_models.ht schema.json --limit 1000", program);
    eprintln!("  {} generate-schema gene_models.ht schema.json", program);
}

fn show_info(table_path: &str) -> Result<()> {
    let metadata_path = hail_decoder::io::join_path(table_path, "metadata.json.gz");

    let mut reader = hail_decoder::io::get_reader(&metadata_path)?;
    let mut data = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut data)?;

    let metadata = hail_decoder::schema::Metadata::from_gzipped_json(&data)?;

    println!("Hail Table Information");
    println!("======================");
    println!("Format version: {}", metadata.file_version);
    println!("Hail version: {}", metadata.hail_version);
    println!("References: {}", metadata.references_rel_path);
    println!("\nFull metadata:");
    println!("{}", serde_json::to_string_pretty(&metadata)?);

    Ok(())
}

fn inspect_table(table_path: &str) -> Result<()> {
    // Load RVD metadata for detailed info
    let rows_metadata_path = hail_decoder::io::join_path(table_path, "rows/metadata.json.gz");

    let rvd_spec = RVDComponentSpec::from_path(&rows_metadata_path)?;

    println!("Hail Table Inspection");
    println!("=====================");
    println!();
    println!("Path: {}", table_path);
    println!();

    // Key information
    println!("Key Fields:");
    for (i, key) in rvd_spec.key.iter().enumerate() {
        println!("  {}. {}", i + 1, key);
    }
    println!();

    // Partition information
    println!("Partitions: {}", rvd_spec.part_files.len());
    if rvd_spec.part_files.len() <= 5 {
        for (i, part) in rvd_spec.part_files.iter().enumerate() {
            println!("  {}. {}", i, part);
        }
    } else {
        for (i, part) in rvd_spec.part_files.iter().take(3).enumerate() {
            println!("  {}. {}", i, part);
        }
        println!("  ... ({} more)", rvd_spec.part_files.len() - 3);
    }
    println!();

    // Index information
    if let Some(ref index_spec) = rvd_spec.index_spec {
        println!("Index: Yes");
        println!("  Path: {}", index_spec.rel_path);
        println!("  Key Type: {}", index_spec.key_type);
    } else {
        println!("Index: No");
    }
    println!();

    // Partition bounds
    println!("Partition Bounds:");
    for (i, interval) in rvd_spec.range_bounds.iter().enumerate() {
        println!("  Partition {}:", i);
        println!("    Start: {}", serde_json::to_string(&interval.start)?);
        println!("    End: {}", serde_json::to_string(&interval.end)?);
    }
    println!();

    // Codec information
    println!("Row Codec:");
    println!("  EType: {}", rvd_spec.codec_spec.e_type);
    println!("  VType: {}", rvd_spec.codec_spec.v_type);

    Ok(())
}

fn run_query(program: &str, args: &[String]) -> Result<()> {
    if args.is_empty() {
        eprintln!("Usage: {} query <table.ht> [options]", program);
        eprintln!("Run '{} help' for more information", program);
        std::process::exit(1);
    }

    let table_path = &args[0];
    let mut key_filters: Vec<(String, String)> = Vec::new();
    let mut where_filters: Vec<KeyRange> = Vec::new();
    let mut limit: Option<usize> = None;
    let mut json_output = false;

    // Parse arguments
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--key" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --key requires a value (field=value)");
                    std::process::exit(1);
                }
                if let Some((field, value)) = parse_equality(&args[i]) {
                    key_filters.push((field, value));
                } else {
                    eprintln!("Error: Invalid --key format. Use field=value");
                    std::process::exit(1);
                }
            }
            "--where" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --where requires a condition");
                    std::process::exit(1);
                }
                if let Some(range) = parse_where_condition(&args[i]) {
                    where_filters.push(range);
                } else {
                    eprintln!("Error: Invalid --where format. Use field=value or field>value");
                    std::process::exit(1);
                }
            }
            "--limit" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --limit requires a number");
                    std::process::exit(1);
                }
                limit = args[i].parse().ok();
                if limit.is_none() {
                    eprintln!("Error: Invalid --limit value");
                    std::process::exit(1);
                }
            }
            "--json" => {
                json_output = true;
            }
            _ => {
                eprintln!("Unknown option: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Open the table (supports both local and cloud paths)
    let mut engine = QueryEngine::open_path(table_path)?;

    println!("Querying table: {}", table_path);
    println!("Key fields: {:?}", engine.key_fields());
    println!();

    // Execute query
    if !key_filters.is_empty() {
        // Point lookup using --key
        let key = build_key_from_filters(&key_filters, engine.key_fields())?;
        println!("Point lookup for key: {:?}", key_filters);

        match engine.lookup(&key)? {
            Some(row) => {
                println!("\nFound row:");
                print_row(&row, json_output)?;
            }
            None => {
                println!("\nNo matching row found.");
            }
        }
    } else {
        // Range query using --where (or full scan if no filters)
        if where_filters.is_empty() {
            println!("Warning: No filters specified. This may scan all partitions.");
        } else {
            println!(
                "Filter conditions: {:?}",
                where_filters
                    .iter()
                    .map(|r| r.field_path_str())
                    .collect::<Vec<_>>()
            );
        }

        println!("\nStreaming results...");

        // Use streaming query for memory-efficient iteration
        let iterator = engine.query_iter(&where_filters)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        let mut count = 0;
        for row_result in iterator {
            let row = row_result?;
            count += 1;
            if !json_output {
                println!("\n--- Row {} ---", count);
            }
            print_row(&row, json_output)?;
        }

        println!("\nRows returned: {}", count);
    }

    Ok(())
}

fn parse_equality(s: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() == 2 {
        Some((parts[0].to_string(), parts[1].to_string()))
    } else {
        None
    }
}

/// Parse a field string into a field path (supports dot notation)
fn parse_field_path(field: &str) -> Vec<String> {
    field.split('.').map(|s| s.to_string()).collect()
}

fn parse_where_condition(s: &str) -> Option<KeyRange> {
    // Try different operators
    if let Some(pos) = s.find(">=") {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 2..]);
        return Some(KeyRange::gte_nested(field_path, value));
    }
    if let Some(pos) = s.find("<=") {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 2..]);
        return Some(KeyRange::lte_nested(field_path, value));
    }
    if let Some(pos) = s.find("gte") {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 3..]);
        return Some(KeyRange::gte_nested(field_path, value));
    }
    if let Some(pos) = s.find("lte") {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 3..]);
        return Some(KeyRange::lte_nested(field_path, value));
    }
    if let Some(pos) = s.find('>') {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange::gt_nested(field_path, value));
    }
    if let Some(pos) = s.find('<') {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange::lt_nested(field_path, value));
    }
    if let Some(pos) = s.find('=') {
        let field_path = parse_field_path(&s[..pos]);
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange::point_nested(field_path, value));
    }

    None
}

fn parse_key_value(s: &str) -> KeyValue {
    // Try to parse as integer first
    if let Ok(i) = s.parse::<i32>() {
        return KeyValue::Int32(i);
    }
    if let Ok(i) = s.parse::<i64>() {
        return KeyValue::Int64(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return KeyValue::Float64(f);
    }
    if s == "true" {
        return KeyValue::Boolean(true);
    }
    if s == "false" {
        return KeyValue::Boolean(false);
    }
    // Default to string
    KeyValue::String(s.to_string())
}

fn build_key_from_filters(
    filters: &[(String, String)],
    key_fields: &[String],
) -> Result<EncodedValue> {
    let mut fields = Vec::new();

    for key_field in key_fields {
        if let Some((_, value)) = filters.iter().find(|(f, _)| f == key_field) {
            // Use heuristics to determine if value should be an integer:
            // - If it looks like a pure integer (no letters), parse as Int32
            // - Otherwise treat as string/binary
            let encoded = if value.chars().all(|c| c.is_ascii_digit() || c == '-')
                && !value.is_empty()
                && value.parse::<i32>().is_ok()
                && !looks_like_string_field(key_field)
            {
                // Parse as integer
                EncodedValue::Int32(value.parse().unwrap())
            } else {
                // Keep as string/binary
                EncodedValue::Binary(value.as_bytes().to_vec())
            };
            fields.push((key_field.clone(), encoded));
        }
    }

    Ok(EncodedValue::Struct(fields))
}

fn looks_like_string_field(field_name: &str) -> bool {
    // Fields that are commonly strings even if they contain only digits
    let string_fields = ["chrom", "chromosome", "contig", "gene_id", "transcript_id", "id"];
    string_fields.iter().any(|&s| field_name.to_lowercase().contains(s))
}

fn print_row(row: &EncodedValue, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", encoded_value_to_json(row));
    } else {
        print_encoded_value(row, 0);
    }
    Ok(())
}

fn print_encoded_value(value: &EncodedValue, indent: usize) {
    let prefix = "  ".repeat(indent);
    match value {
        EncodedValue::Null => println!("{}null", prefix),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b);
            println!("{}\"{}\"", prefix, s)
        }
        EncodedValue::Int32(i) => println!("{}{}", prefix, i),
        EncodedValue::Int64(i) => println!("{}{}", prefix, i),
        EncodedValue::Float32(f) => println!("{}{}", prefix, f),
        EncodedValue::Float64(f) => println!("{}{}", prefix, f),
        EncodedValue::Boolean(b) => println!("{}{}", prefix, b),
        EncodedValue::Struct(fields) => {
            for (name, val) in fields {
                print!("{}{}: ", prefix, name);
                match val {
                    EncodedValue::Struct(_) | EncodedValue::Array(_) => {
                        println!();
                        print_encoded_value(val, indent + 1);
                    }
                    _ => print_encoded_value(val, 0),
                }
            }
        }
        EncodedValue::Array(elements) => {
            println!("{}[", prefix);
            for elem in elements {
                print_encoded_value(elem, indent + 1);
            }
            println!("{}]", prefix);
        }
    }
}

fn encoded_value_to_json(value: &EncodedValue) -> String {
    match value {
        EncodedValue::Null => "null".to_string(),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b);
            format!("\"{}\"", s.replace('\"', "\\\""))
        }
        EncodedValue::Int32(i) => i.to_string(),
        EncodedValue::Int64(i) => i.to_string(),
        EncodedValue::Float32(f) => f.to_string(),
        EncodedValue::Float64(f) => f.to_string(),
        EncodedValue::Boolean(b) => b.to_string(),
        EncodedValue::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|(name, val)| format!("\"{}\":{}", name, encoded_value_to_json(val)))
                .collect();
            format!("{{{}}}", field_strs.join(","))
        }
        EncodedValue::Array(elements) => {
            let elem_strs: Vec<String> = elements.iter().map(encoded_value_to_json).collect();
            format!("[{}]", elem_strs.join(","))
        }
    }
}

fn convert(input: &str, output: &str) -> Result<()> {
    use arrow::record_batch::RecordBatch;
    use hail_decoder::parquet::{build_record_batch, ParquetWriter};

    println!("Converting {} to {}", input, output);

    // Open the query engine to read the table
    let engine = QueryEngine::open_path(input)?;
    let num_partitions = engine.num_partitions();
    let row_type = engine.row_type().clone();

    println!("Table has {} partitions", num_partitions);
    println!("Key fields: {:?}", engine.key_fields());

    // Create the Parquet writer to get the schema
    let mut writer = ParquetWriter::new(output, &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Setup progress bar
    let pb = ProgressBar::new(num_partitions as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} partitions ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Parallel scan: each partition produces a RecordBatch
    let batches: Vec<Result<RecordBatch>> = (0..num_partitions)
        .into_par_iter()
        .map(|i| {
            let rows = engine.scan_partition(i, &[])?;
            let batch = build_record_batch(&rows, &row_type, arrow_schema.clone())?;
            pb.inc(1);
            Ok(batch)
        })
        .collect();

    pb.finish_and_clear();

    // Write all batches sequentially (each becomes a row group)
    let mut total_rows = 0;
    for batch_result in batches {
        let batch = batch_result?;
        total_rows += batch.num_rows();
        writer.write_batch(&batch)?;
    }

    // Close the writer
    writer.close()?;

    // Print summary
    let output_size = std::fs::metadata(output)
        .map(|m| format_bytes(m.len()))
        .unwrap_or_else(|_| "unknown".to_string());

    println!();
    println!("Conversion complete!");
    println!("  Rows written: {}", total_rows);
    println!("  Output file: {}", output);
    println!("  Output size: {}", output_size);

    Ok(())
}

/// Format bytes into a human-readable string
fn format_bytes(bytes: u64) -> String {
    const UNIT: u64 = 1024;
    if bytes < UNIT {
        return format!("{} B", bytes);
    }
    if bytes < UNIT.pow(2) {
        return format!("{:.2} KiB", bytes as f64 / UNIT as f64);
    }
    if bytes < UNIT.pow(3) {
        return format!("{:.2} MiB", bytes as f64 / UNIT.pow(2) as f64);
    }
    format!("{:.2} GiB", bytes as f64 / UNIT.pow(3) as f64)
}

/// Run the summary command
fn run_summary(table_path: &str) -> Result<()> {
    let engine = QueryEngine::open_path(table_path)?;
    let rvd = engine.rvd_spec().clone();
    let part_count = engine.num_partitions();

    // Print header
    println!("Hail Table Summary");
    println!("==================");
    println!();

    // Basic info
    let name = std::path::Path::new(table_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    println!("Name: {}", name);
    println!("Path: {}", table_path);
    println!("Partitions: {}", part_count);
    println!();

    // Key fields
    println!("Key Fields:");
    for (i, key) in rvd.key.iter().enumerate() {
        println!("  {}. {}", i + 1, key);
    }
    println!();

    // Calculate partition sizes (parallel)
    println!("Calculating partition sizes (parallel)...");
    let pb = ProgressBar::new(part_count as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} partitions")
            .unwrap()
            .progress_chars("#>-"),
    );

    let parts_dir = join_path(&join_path(table_path, "rows"), "parts");

    let sizes: Vec<u64> = rvd.part_files
        .par_iter()
        .map(|part| {
            let path = join_path(&parts_dir, part);
            let size = get_file_size(&path).unwrap_or(0);
            pb.inc(1);
            size
        })
        .collect();

    pb.finish_and_clear();

    let total_size: u64 = sizes.iter().sum();
    let mean_size = if part_count > 0 {
        total_size as f64 / part_count as f64
    } else {
        0.0
    };

    // Calculate standard deviation
    let variance = if part_count > 1 {
        let mean = mean_size;
        sizes.iter().map(|&s| {
            let diff = s as f64 - mean;
            diff * diff
        }).sum::<f64>() / (part_count - 1) as f64
    } else {
        0.0
    };
    let std_dev = variance.sqrt();

    println!("Size Statistics:");
    println!("  Total Size: {}", format_bytes(total_size));
    println!("  Mean Partition Size: {}", format_bytes(mean_size as u64));
    println!("  Std Dev: {}", format_bytes(std_dev as u64));
    println!();

    // Schema
    println!("Schema:");
    println!("----------------------------------------");
    println!("{}", format_schema_clean(&rvd.codec_spec.v_type));
    println!("----------------------------------------");
    println!();

    // Data scan for statistics (parallel)
    println!("Scanning data for field statistics (parallel)...");
    let pb = ProgressBar::new(part_count as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} partitions ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let total_rows = AtomicUsize::new(0);

    // Parallel scan using rayon - each thread gets its own StatsAccumulator
    let stats = (0..part_count)
        .into_par_iter()
        .fold(
            || StatsAccumulator::new(),
            |mut acc, i| {
                match engine.scan_partition(i, &[]) {
                    Ok(rows) => {
                        total_rows.fetch_add(rows.len(), Ordering::Relaxed);
                        for row in &rows {
                            acc.process_row(row);
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to scan partition {}: {}", i, e);
                    }
                }
                pb.inc(1);
                acc
            },
        )
        .reduce(
            || StatsAccumulator::new(),
            |mut a, b| {
                a.merge(b);
                a
            },
        );

    pb.finish_with_message("Scan complete");
    let total_rows = total_rows.load(Ordering::Relaxed);

    println!();
    println!("Row Count: {}", total_rows);
    println!();

    // Print field statistics
    println!("Field Statistics:");
    println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
        "Field", "Count", "Nulls", "Min", "Max");
    println!("{}", "-".repeat(120));

    for key in stats.sorted_fields() {
        let s = &stats.stats[key];

        // Truncate field name if too long
        let field_display = if key.len() > 48 {
            format!("...{}", &key[key.len() - 45..])
        } else {
            key.clone()
        };

        // Truncate min/max if too long
        let min_display = match &s.min {
            Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
            Some(m) => m.clone(),
            None => String::new(),
        };
        let max_display = match &s.max {
            Some(m) if m.len() > 18 => format!("{}...", &m[..15]),
            Some(m) => m.clone(),
            None => String::new(),
        };

        println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
            field_display,
            s.count,
            s.null_count,
            min_display,
            max_display
        );
    }

    Ok(())
}

/// Run the validate command
fn run_validate(program: &str, args: &[String]) -> Result<()> {
    if args.len() < 2 {
        eprintln!("Usage: {} validate <table.ht> <schema.json> [options]", program);
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --limit <n>     Validate first N rows (sequential)");
        eprintln!("  --sample <n>    Validate N randomly sampled rows");
        eprintln!("  --fail-fast     Stop on first validation error");
        std::process::exit(1);
    }

    let table_path = &args[0];
    let schema_path = &args[1];
    let mut limit: Option<usize> = None;
    let mut sample: Option<usize> = None;
    let mut fail_fast = false;

    // Parse optional arguments
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--limit" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --limit requires a number");
                    std::process::exit(1);
                }
                limit = args[i].parse().ok();
                if limit.is_none() {
                    eprintln!("Error: Invalid --limit value");
                    std::process::exit(1);
                }
            }
            "--sample" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --sample requires a number");
                    std::process::exit(1);
                }
                sample = args[i].parse().ok();
                if sample.is_none() {
                    eprintln!("Error: Invalid --sample value");
                    std::process::exit(1);
                }
            }
            "--fail-fast" => {
                fail_fast = true;
            }
            _ => {
                eprintln!("Unknown option: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Validate that --limit and --sample aren't both specified
    if limit.is_some() && sample.is_some() {
        eprintln!("Error: Cannot use both --limit and --sample. Choose one.");
        std::process::exit(1);
    }

    println!("Validating table: {}", table_path);
    println!("Using schema: {}", schema_path);
    if let Some(l) = limit {
        println!("Row limit: {} (sequential)", l);
    }
    if let Some(s) = sample {
        println!("Sample size: {} (random)", s);
    }
    if fail_fast {
        println!("Mode: fail-fast");
    }
    println!();

    // Load the JSON schema
    let validator = SchemaValidator::from_file(schema_path)?;

    // Open the table
    let engine = QueryEngine::open_path(table_path)?;

    println!("Table has {} partitions", engine.num_partitions());
    println!();

    // Run validation
    println!("Running validation...");
    let report = if let Some(sample_size) = sample {
        validator.validate_sample(&engine, sample_size, fail_fast)?
    } else {
        validator.validate(&engine, limit, fail_fast)?
    };

    // Print results
    println!();
    println!("{}", report);

    // Exit with error code if validation failed
    if report.invalid_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Run the generate-schema command
fn run_generate_schema(program: &str, args: &[String]) -> Result<()> {
    if args.is_empty() {
        eprintln!("Usage: {} generate-schema <table.ht> [output.json]", program);
        eprintln!();
        eprintln!("If output.json is not specified, the schema is printed to stdout.");
        std::process::exit(1);
    }

    let table_path = &args[0];
    let output_path = args.get(1);

    println!("Generating JSON schema for: {}", table_path);

    // Open the table
    let engine = QueryEngine::open_path(table_path)?;

    // Get the table name from path for title
    let title = std::path::Path::new(table_path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_end_matches(".ht"));

    // Generate the schema
    let schema = SchemaGenerator::from_engine(&engine, title)?;

    if let Some(path) = output_path {
        // Write to file
        SchemaGenerator::write_to_file(&schema, path)?;
        println!("Schema written to: {}", path);
    } else {
        // Print to stdout
        println!();
        println!("{}", serde_json::to_string_pretty(&schema)?);
    }

    Ok(())
}

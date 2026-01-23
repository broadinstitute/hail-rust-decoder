//! Hail decoder CLI tool
//!
//! Commands:
//! - info: Show table metadata, keys, partition layout, and schema (fast)
//! - query: Stream rows with optional filtering (lazy)
//! - summary: Scan full dataset to calculate row counts and field statistics (slow)
//! - export: Export data to other formats (Parquet, ClickHouse, BigQuery)

mod cli;

use clap::Parser;
use cli::{Cli, Commands, ExportCommands, ExportParquetArgs, HasCommonExportArgs, QueryArgs};
#[cfg(feature = "validation")]
use cli::{SchemaSubcommands, ValidateArgs};
#[cfg(feature = "clickhouse")]
use cli::ExportClickhouseArgs;
use hail_decoder::codec::EncodedValue;
use hail_decoder::io::{get_file_size, join_path};
use hail_decoder::query::{IntervalList, KeyRange, KeyValue, QueryEngine};
use hail_decoder::summary::{format_schema_clean, StatsAccumulator};
use std::sync::Arc;
#[cfg(feature = "validation")]
use hail_decoder::validation::{SchemaGenerator, SchemaValidator};
use hail_decoder::Result;
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Info { path } => show_info(&path)?,
        Commands::Summary { path } => run_summary(&path)?,
        Commands::Query(args) => run_query(args)?,
        Commands::Export { command } => match command {
            ExportCommands::Parquet(args) => run_export_parquet(args)?,
            #[cfg(feature = "clickhouse")]
            ExportCommands::Clickhouse(args) => run_export_clickhouse(args)?,
            #[cfg(feature = "bigquery")]
            ExportCommands::Bigquery(args) => run_export_bigquery(args)?,
        },
        #[cfg(feature = "validation")]
        Commands::Schema { command } => match command {
            SchemaSubcommands::Validate(args) => run_validate(args)?,
            SchemaSubcommands::Generate(args) => run_generate_schema(&args.table, args.output.as_deref())?,
        },
    }

    Ok(())
}

/// Create a standard progress bar style (no emojis)
fn progress_style_bar() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-")
}

/// Create a standard spinner style (no emojis)
fn progress_style_spinner() -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template("{spinner:.green} {msg}")
        .unwrap()
}

/// Parse where filters from any export args implementing HasCommonExportArgs.
/// This enforces at compile time that all export targets have common args.
fn parse_export_filters(args: &impl HasCommonExportArgs) -> Vec<KeyRange> {
    let mut filters = Vec::new();
    for clause in &args.common().where_clauses {
        if let Some(range) = parse_where_condition(clause) {
            filters.push(range);
        } else {
            eprintln!("{} Invalid --where format: {}", "Error:".red().bold(), clause);
            std::process::exit(1);
        }
    }
    filters
}

/// Parse interval list from CLI arguments (file and/or strings)
///
/// # Arguments
/// * `file` - Optional path to interval file (.bed, .json, or text)
/// * `strings` - Optional list of interval strings (chr:start-end format)
///
/// # Returns
/// * `Ok(None)` if no intervals specified
/// * `Ok(Some(Arc<IntervalList>))` with merged and optimized intervals
/// * `Err` on parse errors
fn parse_interval_list(
    file: Option<&str>,
    strings: &[String],
) -> Result<Option<Arc<IntervalList>>> {
    // Return None if no intervals specified
    if file.is_none() && strings.is_empty() {
        return Ok(None);
    }

    let mut list = IntervalList::new();

    // Load from file if specified
    if let Some(path) = file {
        let file_list = IntervalList::from_file(path)?;
        list.merge(file_list);
    }

    // Parse string intervals if specified
    if !strings.is_empty() {
        let string_list = IntervalList::from_strings(strings)?;
        list.merge(string_list);
    }

    // Optimize the combined list
    list.optimize();

    Ok(Some(Arc::new(list)))
}

/// Parse interval list from export args
fn parse_export_intervals(args: &impl HasCommonExportArgs) -> Result<Option<Arc<IntervalList>>> {
    parse_interval_list(
        args.common().intervals_file.as_deref(),
        &args.common().interval,
    )
}

fn show_info(table_path: &str) -> Result<()> {
    // Check if this is a VCF file
    if table_path.ends_with(".vcf") || table_path.ends_with(".vcf.gz") || table_path.ends_with(".vcf.bgz") {
        println!("{}", "VCF File Information".bold().underline());
        println!();
        println!("{} {}", "Path:".green(), table_path.bright_white());
        println!();

        // Open using query engine to get schema info
        let engine = QueryEngine::open_path(table_path)?;

        println!("{} {}", "Contigs:".green(), engine.num_partitions().to_string().bright_white());
        println!("{}", "Row Schema:".green());
        println!("{:?}", engine.row_type());
        return Ok(());
    }

    // Hail Table - Read basic metadata first
    let metadata_path = hail_decoder::io::join_path(table_path, "metadata.json.gz");

    // Try reading metadata (might fail if not a hail table)
    let metadata = match hail_decoder::io::get_reader(&metadata_path) {
        Ok(mut reader) => {
            let mut data = Vec::new();
            std::io::Read::read_to_end(&mut reader, &mut data)?;
            hail_decoder::schema::Metadata::from_gzipped_json(&data)?
        },
        Err(_) => {
            println!("{} Not a valid Hail table or VCF file", "Error:".red());
            return Ok(());
        }
    };

    println!("{}", "Hail Table Information".bold().underline());
    println!();
    println!("{} {}", "Path:".green(), table_path.bright_white());
    println!("{} {}", "Format version:".green(), metadata.file_version.bright_white());
    println!("{} {}", "Hail version:".green(), metadata.hail_version.bright_white());
    println!("{} {}", "References:".green(), metadata.references_rel_path.bright_white());
    println!();

    // Open using query engine for structural inspection
    let engine = QueryEngine::open_path(table_path)?;

    // Key information
    println!("{}", "Key Fields:".green());
    let keys = engine.key_fields();
    if keys.is_empty() {
        println!("  {}", "(none)".dimmed());
    } else {
        for (i, key) in keys.iter().enumerate() {
            println!("  {}. {}", (i + 1).to_string().cyan(), key.bright_white());
        }
    }
    println!();

    // Partition information
    println!("{} {}", "Partitions:".green(), engine.num_partitions().to_string().bright_white());
    if engine.has_index() {
        println!("{} {}", "Index:".green(), "Yes".bright_green());
    } else {
        println!("{} {}", "Index:".green(), "No".yellow());
    }
    println!();

    // Hail-specific structural information
    if let Some(rvd_spec) = engine.rvd_spec() {
        // Partition files
        if rvd_spec.part_files.len() <= 5 {
            println!("{}", "Partition Files:".green());
            for (i, part) in rvd_spec.part_files.iter().enumerate() {
                println!("  {}. {}", i.to_string().cyan(), part.dimmed());
            }
        } else {
            println!("{}", "Partition Files (first 5):".green());
            for (i, part) in rvd_spec.part_files.iter().take(5).enumerate() {
                println!("  {}. {}", i.to_string().cyan(), part.dimmed());
            }
            println!("  {} ({} more)", "...".dimmed(), rvd_spec.part_files.len() - 5);
        }
        println!();

        // Index information
        if let Some(ref index_spec) = rvd_spec.index_spec {
            println!("{}", "Index Details:".green());
            println!("  {} {}", "Path:".cyan(), index_spec.rel_path.dimmed());
            println!("  {} {}", "Key Type:".cyan(), index_spec.key_type.dimmed());
            println!();
        }

        // Partition bounds (sample)
        println!("{}", "Partition Bounds (first 3):".green());
        for (i, interval) in rvd_spec.range_bounds.iter().take(3).enumerate() {
            println!("  {} {}:", "Partition".cyan(), i);
            // Just show start/end JSON cleanly
            let start = serde_json::to_string(&interval.start).unwrap_or_default();
            let end = serde_json::to_string(&interval.end).unwrap_or_default();
            println!("    {} .. {}", start.dimmed(), end.dimmed());
        }
        if rvd_spec.range_bounds.len() > 3 {
            println!("    {}", "...".dimmed());
        }
        println!();

        // Codec information
        println!("{}", "Row Codec:".green());
        // Clean format the VType
        println!("{}", format_schema_clean(&rvd_spec.codec_spec.v_type));
    }

    Ok(())
}

fn run_query(args: QueryArgs) -> Result<()> {
    let table_path = &args.table;
    let mut key_filters: Vec<(String, String)> = Vec::new();
    let mut where_filters: Vec<KeyRange> = Vec::new();

    // Parse --key if provided
    if let Some(ref key_str) = args.key {
        if let Some((field, value)) = parse_equality(key_str) {
            key_filters.push((field, value));
        } else {
            eprintln!("{} Invalid --key format. Use field=value", "Error:".red().bold());
            std::process::exit(1);
        }
    }

    // Parse --where clauses
    for clause in &args.where_clauses {
        if let Some(range) = parse_where_condition(clause) {
            where_filters.push(range);
        } else {
            eprintln!("{} Invalid --where format: {}", "Error:".red().bold(), clause);
            std::process::exit(1);
        }
    }

    // Parse interval list
    let intervals = parse_interval_list(args.intervals_file.as_deref(), &args.interval)?;

    // Open the table (supports both local and cloud paths)
    let mut engine = QueryEngine::open_path(table_path)?;

    println!("{} {}", "Querying table:".green(), table_path.bright_white());
    println!("{} {:?}", "Key fields:".green(), engine.key_fields());
    if let Some(ref ivl) = intervals {
        println!("{} {} intervals", "Interval filter:".green(), ivl.len().to_string().bright_white());
    }
    println!();

    // Execute query
    if !key_filters.is_empty() {
        // Point lookup using --key
        let key = build_key_from_filters(&key_filters, engine.key_fields())?;
        println!("{} {:?}", "Point lookup for key:".cyan(), key_filters);

        match engine.lookup(&key)? {
            Some(row) => {
                // Apply interval filter to lookup result if specified
                if let Some(ref ivl) = intervals {
                    if !row_matches_intervals(&row, ivl) {
                        println!();
                        println!("{}", "Row found but filtered out by interval list.".yellow());
                        return Ok(());
                    }
                }
                println!();
                println!("{}", "Found row:".green().bold());
                print_row(&row, args.json)?;
            }
            None => {
                println!();
                println!("{}", "No matching row found.".yellow());
            }
        }
    } else {
        // Range query using --where (or full scan if no filters)
        if where_filters.is_empty() && intervals.is_none() {
            println!("{}", "Warning: No filters specified. This may scan all partitions.".yellow());
        } else {
            if !where_filters.is_empty() {
                println!(
                    "{} {:?}",
                    "Filter conditions:".cyan(),
                    where_filters
                        .iter()
                        .map(|r| r.field_path_str())
                        .collect::<Vec<_>>()
                );
            }
        }

        println!();
        println!("{}", "Streaming results...".dimmed());

        // Use streaming query with intervals for memory-efficient iteration
        let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

        // Apply limit if specified
        let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.limit {
            Box::new(iterator.take(n))
        } else {
            Box::new(iterator)
        };

        let mut count = 0;
        for row_result in iterator {
            let row = row_result?;
            count += 1;
            if !args.json {
                println!();
                println!("{} {} {}", "---".dimmed(), format!("Row {}", count).cyan(), "---".dimmed());
            }
            print_row(&row, args.json)?;
        }

        println!();
        println!("{} {}", "Rows returned:".green(), count.to_string().bright_white());
    }

    Ok(())
}

/// Check if a row's locus matches any interval (used for point lookup filtering)
fn row_matches_intervals(row: &EncodedValue, intervals: &IntervalList) -> bool {
    // Extract locus.contig and locus.position from the row
    if let EncodedValue::Struct(fields) = row {
        if let Some((_, locus)) = fields.iter().find(|(name, _)| name == "locus") {
            if let EncodedValue::Struct(locus_fields) = locus {
                let contig = locus_fields.iter().find(|(name, _)| name == "contig").map(|(_, v)| v);
                let position = locus_fields.iter().find(|(name, _)| name == "position").map(|(_, v)| v);

                if let (Some(EncodedValue::Binary(c)), Some(EncodedValue::Int32(p))) = (contig, position) {
                    let contig_str = String::from_utf8_lossy(c);
                    return intervals.contains(&contig_str, *p);
                }
            }
        }
    }
    // If we can't extract locus, pass through
    true
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
        EncodedValue::Null => println!("{}{}", prefix, "null".dimmed()),
        EncodedValue::Binary(b) => {
            let s = String::from_utf8_lossy(b);
            println!("{}\"{}\"", prefix, s.bright_white())
        }
        EncodedValue::Int32(i) => println!("{}{}", prefix, i.to_string().cyan()),
        EncodedValue::Int64(i) => println!("{}{}", prefix, i.to_string().cyan()),
        EncodedValue::Float32(f) => println!("{}{}", prefix, f.to_string().cyan()),
        EncodedValue::Float64(f) => println!("{}{}", prefix, f.to_string().cyan()),
        EncodedValue::Boolean(b) => {
            if *b {
                println!("{}{}", prefix, "true".green());
            } else {
                println!("{}{}", prefix, "false".yellow());
            }
        }
        EncodedValue::Struct(fields) => {
            for (name, val) in fields {
                print!("{}{}: ", prefix, name.green());
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

fn run_export_parquet(args: ExportParquetArgs) -> Result<()> {
    use hail_decoder::parquet::{build_record_batch, ParquetWriter};

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!("{} {} {} {}", "Converting".green(), args.common.input.bright_white(), "to".green(), args.output.bright_white());

    // Open the query engine
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    println!("{} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!("{} {:?}", "Key fields:".cyan(), engine.key_fields());
    if !where_filters.is_empty() {
        println!("{} {:?}", "Filters:".cyan(), where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>());
    }
    if let Some(ref ivl) = intervals {
        println!("{} {} intervals", "Interval filter:".cyan(), ivl.len().to_string().bright_white());
    }
    if let Some(l) = args.common.limit {
        println!("{} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    println!();

    // Create writer and get schema
    let mut writer = ParquetWriter::new(&args.output, &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Use streaming query with filters and intervals
    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

    // Apply limit if specified
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // Collect rows in batches for efficient parquet writing
    let batch_size = 10000;
    let mut batch_rows = Vec::with_capacity(batch_size);
    let mut total_rows = 0;

    // Progress indicator
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        batch_rows.push(row);
        total_rows += 1;

        if batch_rows.len() >= batch_size {
            pb.set_message(format!("{} rows processed...", total_rows));
            let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
            writer.write_batch(&batch)?;
            batch_rows.clear();
        }
    }

    // Write remaining rows
    if !batch_rows.is_empty() {
        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
        writer.write_batch(&batch)?;
    }

    pb.finish_and_clear();
    writer.close()?;

    // Print summary
    let output_size = std::fs::metadata(&args.output)
        .map(|m| format_bytes(m.len()))
        .unwrap_or_else(|_| "unknown".to_string());

    println!();
    println!("{}", "Conversion complete!".green().bold());
    println!("  {} {}", "Rows written:".cyan(), total_rows.to_string().bright_white());
    println!("  {} {}", "Output file:".cyan(), args.output.bright_white());
    println!("  {} {}", "Output size:".cyan(), output_size.bright_white());

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
    let part_count = engine.num_partitions();

    // Print header
    println!("{}", "Table Summary".bold().underline());
    println!();

    // Basic info
    let name = std::path::Path::new(table_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    println!("{} {}", "Name:".green(), name.bright_white());
    println!("{} {}", "Path:".green(), table_path.bright_white());
    println!("{} {}", "Partitions:".green(), part_count.to_string().bright_white());
    println!();

    // Key fields
    println!("{}", "Key Fields:".green());
    let keys = engine.key_fields();
    if keys.is_empty() {
        println!("  {}", "(none)".dimmed());
    } else {
        for (i, key) in keys.iter().enumerate() {
            println!("  {}. {}", (i + 1).to_string().cyan(), key.bright_white());
        }
    }
    println!();

    // Hail-specific partition size calculation
    if let Some(rvd) = engine.rvd_spec() {
        // Calculate partition sizes (parallel)
        println!("{}", "Calculating partition sizes...".dimmed());
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

        println!("{}", "Size Statistics:".green());
        println!("  {} {}", "Total Size:".cyan(), format_bytes(total_size).bright_white());
        println!("  {} {}", "Mean Partition Size:".cyan(), format_bytes(mean_size as u64).bright_white());
        println!("  {} {}", "Std Dev:".cyan(), format_bytes(std_dev as u64).bright_white());
        println!();

        // Schema
        println!("{}", "Schema:".green());
        println!("{}", "-".repeat(40).dimmed());
        println!("{}", format_schema_clean(&rvd.codec_spec.v_type));
        println!("{}", "-".repeat(40).dimmed());
        println!();
    } else {
        // VCF or other non-Hail source
        println!("{}", "(Size statistics not available for this format)".dimmed());
        println!();
    }

    // Data scan for statistics (parallel)
    println!("{}", "Scanning data for field statistics...".dimmed());
    let pb = ProgressBar::new(part_count as u64);
    pb.set_style(progress_style_bar());

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
                        eprintln!("{} Failed to scan partition {}: {}", "Warning:".yellow(), i, e);
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

    pb.finish_and_clear();
    let total_rows = total_rows.load(Ordering::Relaxed);

    println!();
    println!("{} {}", "Row Count:".green(), total_rows.to_string().bright_white().bold());
    println!();

    // Print field statistics
    println!("{}", "Field Statistics:".green().bold());
    println!("{:<50} | {:>10} | {:>10} | {:>20} | {:>20}",
        "Field".cyan(), "Count".cyan(), "Nulls".cyan(), "Min".cyan(), "Max".cyan());
    println!("{}", "-".repeat(120).dimmed());

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
#[cfg(feature = "validation")]
fn run_validate(args: ValidateArgs) -> Result<()> {
    // Validate that --limit and --sample aren't both specified
    if args.limit.is_some() && args.sample.is_some() {
        eprintln!("{} Cannot use both --limit and --sample. Choose one.", "Error:".red().bold());
        std::process::exit(1);
    }

    println!("{} {}", "Validating table:".green(), args.table.bright_white());
    println!("{} {}", "Using schema:".green(), args.schema.bright_white());
    if let Some(l) = args.limit {
        println!("{} {} {}", "Row limit:".cyan(), l.to_string().bright_white(), "(sequential)".dimmed());
    }
    if let Some(s) = args.sample {
        println!("{} {} {}", "Sample size:".cyan(), s.to_string().bright_white(), "(random)".dimmed());
    }
    if args.fail_fast {
        println!("{} {}", "Mode:".cyan(), "fail-fast".yellow());
    }
    println!();

    // Load the JSON schema
    let validator = SchemaValidator::from_file(&args.schema)?;

    // Open the table
    let engine = QueryEngine::open_path(&args.table)?;

    println!("{} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!();

    // Run validation
    let report = if let Some(sample_size) = args.sample {
        if args.verbose {
            validator.validate_sample_verbose(&engine, sample_size, args.fail_fast)?
        } else {
            validator.validate_sample(&engine, sample_size, args.fail_fast)?
        }
    } else {
        println!("{}", "Validating rows sequentially...".dimmed());
        validator.validate(&engine, args.limit, args.fail_fast)?
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
#[cfg(feature = "validation")]
fn run_generate_schema(table_path: &str, output_path: Option<&str>) -> Result<()> {
    println!("{} {}", "Generating JSON schema for:".green(), table_path.bright_white());

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
        println!("{} {}", "Schema written to:".green(), path.bright_white());
    } else {
        // Print to stdout
        println!();
        println!("{}", serde_json::to_string_pretty(&schema)?);
    }

    Ok(())
}

/// Export a Hail table to ClickHouse with optional filtering
#[cfg(feature = "clickhouse")]
fn run_export_clickhouse(args: ExportClickhouseArgs) -> Result<()> {
    use hail_decoder::export::clickhouse::generate_create_table;
    use hail_decoder::export::ClickHouseClient;
    use hail_decoder::parquet::{build_record_batch, ParquetWriter};
    use uuid::Uuid;

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!("{} {}", "Exporting to ClickHouse:".green().bold(), args.common.input.bright_white());
    println!("  {} {}", "ClickHouse URL:".cyan(), args.url.bright_white());
    println!("  {} {}", "Target table:".cyan(), args.table.bright_white());
    if !where_filters.is_empty() {
        println!("  {} {:?}", "Filters:".cyan(), where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>());
    }
    if let Some(ref ivl) = intervals {
        println!("  {} {} intervals", "Interval filter:".cyan(), ivl.len().to_string().bright_white());
    }
    if let Some(l) = args.common.limit {
        println!("  {} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    println!();

    // Step 1: Open the query engine
    println!("{}", "Reading table metadata...".dimmed());
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    println!("  {} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!("  {} {:?}", "Key fields:".cyan(), engine.key_fields());
    println!();

    // Step 2: Create ClickHouse client and generate DDL
    let client = ClickHouseClient::new(&args.url);

    println!("{}", "Generating CREATE TABLE DDL...".dimmed());
    let ddl = generate_create_table(&args.table, &row_type, engine.key_fields())
        .map_err(|e| hail_decoder::HailError::InvalidFormat(e.to_string()))?;
    println!("{}", ddl.dimmed());
    println!();

    // Step 3: Execute CREATE TABLE
    println!("{}", "Creating table in ClickHouse...".dimmed());
    client
        .execute(&ddl)
        .map_err(|e| hail_decoder::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))?;
    println!("  {}", "Table created (or already exists)".green());
    println!();

    // Step 4: Convert filtered rows to temporary Parquet file
    let temp_path = format!("/tmp/hail_export_{}.parquet", Uuid::new_v4());
    println!("{}", "Converting to temporary Parquet file...".dimmed());
    println!("  {} {}", "Temp file:".cyan(), temp_path.dimmed());

    // Create writer and get schema
    let mut writer = ParquetWriter::new(&temp_path, &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Use streaming query with filters and intervals
    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

    // Apply limit if specified
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // Collect rows in batches for efficient parquet writing
    let batch_size = 10000;
    let mut batch_rows = Vec::with_capacity(batch_size);
    let mut total_rows = 0;

    // Progress indicator
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        batch_rows.push(row);
        total_rows += 1;

        if batch_rows.len() >= batch_size {
            pb.set_message(format!("{} rows processed...", total_rows));
            let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
            writer.write_batch(&batch)?;
            batch_rows.clear();
        }
    }

    // Write remaining rows
    if !batch_rows.is_empty() {
        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
        writer.write_batch(&batch)?;
    }

    pb.finish_and_clear();
    writer.close()?;
    println!("  {} {}", "Converted".green(), format!("{} rows", total_rows).bright_white());
    println!();

    // Step 5: Insert Parquet data into ClickHouse
    println!("{}", "Inserting data into ClickHouse...".dimmed());
    client
        .insert_parquet(&args.table, &temp_path)
        .map_err(|e| hail_decoder::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))?;

    // Step 6: Clean up temp file
    if let Err(e) = std::fs::remove_file(&temp_path) {
        eprintln!("{} Failed to remove temp file {}: {}", "Warning:".yellow(), temp_path, e);
    }

    // Step 7: Verify
    let row_count = client
        .count_rows(&args.table)
        .map_err(|e| hail_decoder::HailError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!("  {} {}", "Rows in ClickHouse table:".cyan(), row_count.to_string().bright_white());

    Ok(())
}

/// Export a Hail table to BigQuery via GCS staging
#[cfg(feature = "bigquery")]
fn run_export_bigquery(args: cli::ExportBigqueryArgs) -> Result<()> {
    use hail_decoder::export::BigQueryClient;
    use hail_decoder::parquet::{build_record_batch, ParquetWriter};
    use uuid::Uuid;

    // Parse destination: project:dataset.table
    let (project, dataset_table) = args.destination.split_once(':').ok_or_else(|| {
        hail_decoder::HailError::InvalidFormat(
            "Destination format must be project:dataset.table".to_string(),
        )
    })?;
    let (dataset, table) = dataset_table.split_once('.').ok_or_else(|| {
        hail_decoder::HailError::InvalidFormat(
            "Destination format must be project:dataset.table".to_string(),
        )
    })?;

    let where_filters = parse_export_filters(&args);
    let intervals = parse_export_intervals(&args)?;

    println!("{} {} {} {}:{}.{}",
        "Exporting".green().bold(),
        args.common.input.bright_white(),
        "to BigQuery".green(),
        project, dataset, table);
    println!("  {} {}", "Staging bucket:".cyan(), args.bucket.bright_white());
    if !where_filters.is_empty() {
        println!("  {} {:?}", "Filters:".cyan(), where_filters.iter().map(|r| r.field_path_str()).collect::<Vec<_>>());
    }
    if let Some(ref ivl) = intervals {
        println!("  {} {} intervals", "Interval filter:".cyan(), ivl.len().to_string().bright_white());
    }
    if let Some(l) = args.common.limit {
        println!("  {} {}", "Row limit:".cyan(), l.to_string().bright_white());
    }
    println!();

    // 1. Get Schema/Metadata
    println!("{}", "Reading table metadata...".dimmed());
    let engine = QueryEngine::open_path(&args.common.input)?;
    let row_type = engine.row_type().clone();
    println!("  {} {}", "Partitions:".cyan(), engine.num_partitions().to_string().bright_white());
    println!("  {} {:?}", "Key fields:".cyan(), engine.key_fields());
    println!();

    // 2. Convert filtered rows to Parquet locally
    let temp_file_path = std::path::Path::new(&args.temp_dir).join(format!("{}.parquet", Uuid::new_v4()));
    println!("{} {}", "Converting to temporary Parquet file:".dimmed(), temp_file_path.display());

    // Create writer and get schema
    let mut writer = ParquetWriter::new(temp_file_path.to_string_lossy().as_ref(), &row_type)?;
    let arrow_schema = writer.schema().clone();

    // Use streaming query with filters and intervals
    let iterator = engine.query_iter_with_intervals(&where_filters, intervals)?;

    // Apply limit if specified
    let iterator: Box<dyn Iterator<Item = _>> = if let Some(n) = args.common.limit {
        Box::new(iterator.take(n))
    } else {
        Box::new(iterator)
    };

    // Collect rows in batches for efficient parquet writing
    let batch_size = 10000;
    let mut batch_rows = Vec::with_capacity(batch_size);
    let mut rows_written = 0;

    // Progress indicator
    let pb = ProgressBar::new_spinner();
    pb.set_style(progress_style_spinner());

    for row_result in iterator {
        let row = row_result?;
        batch_rows.push(row);
        rows_written += 1;

        if batch_rows.len() >= batch_size {
            pb.set_message(format!("{} rows processed...", rows_written));
            let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
            writer.write_batch(&batch)?;
            batch_rows.clear();
        }
    }

    // Write remaining rows
    if !batch_rows.is_empty() {
        let batch = build_record_batch(&batch_rows, &row_type, arrow_schema.clone())?;
        writer.write_batch(&batch)?;
    }

    pb.finish_and_clear();
    writer.close()?;
    println!("  {} {}", "Converted".green(), format!("{} rows", rows_written).bright_white());
    println!();

    // 3. Upload and Load (Async)
    println!("{}", "Starting BigQuery export...".dimmed());
    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let client = BigQueryClient::new(project, &args.bucket)
            .await
            .map_err(|e| {
                hail_decoder::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        println!("{}", "Uploading to GCS...".dimmed());
        let gcs_uri = client.upload_parquet(&temp_file_path).await.map_err(|e| {
            hail_decoder::HailError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        println!("  {} {}", "Uploaded to:".green(), gcs_uri.bright_white());

        println!("{}", "Triggering BigQuery Load Job...".dimmed());
        client
            .load_parquet(dataset, table, &gcs_uri, &row_type)
            .await
            .map_err(|e| {
                hail_decoder::HailError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
        println!("  {}", "Load Job completed successfully.".green());

        println!("{}", "Cleaning up GCS staging object...".dimmed());
        let _ = client.delete_object(&gcs_uri).await;

        Ok::<(), hail_decoder::HailError>(())
    });

    // 4. Cleanup Local temp file
    if std::fs::remove_file(&temp_file_path).is_err() {
        eprintln!("{} Failed to remove local temp file", "Warning:".yellow());
    }

    // Propagate any errors from the async block
    result?;

    println!();
    println!("{}", "Export complete!".green().bold());
    println!("  {} {}", "Rows exported:".cyan(), rows_written.to_string().bright_white());
    println!("  {} {}:{}.{}", "BigQuery table:".cyan(), project, dataset, table);

    Ok(())
}

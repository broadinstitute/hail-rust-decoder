//! Hail decoder CLI tool
//!
//! Commands:
//! - info: Show basic table metadata
//! - inspect: Show detailed table information including keys and index status
//! - query: Query a table with optional key filters

use hail_decoder::codec::EncodedValue;
use hail_decoder::metadata::RVDComponentSpec;
use hail_decoder::query::{KeyRange, KeyValue, QueryEngine};
use hail_decoder::{HailError, Result};

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
    eprintln!("  query <table.ht> [options]   Query the table");
    eprintln!("  convert <table.ht> <out>     Convert to Parquet (not implemented)");
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
    eprintln!("Examples:");
    eprintln!("  {} inspect gene_models.ht", program);
    eprintln!("  {} query gene_models.ht --key gene_id=ENSG00000141510", program);
    eprintln!("  {} query gene_models.ht --where chrom=chr1 --limit 10", program);
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
            println!("Filter conditions: {:?}", where_filters.iter().map(|r| &r.field).collect::<Vec<_>>());
        }

        let result = engine.query(&where_filters)?;

        println!("\nQuery Statistics:");
        println!("  Partitions scanned: {}", result.partitions_scanned);
        println!("  Partitions pruned: {}", result.partitions_pruned);
        println!("  Rows found: {}", result.rows.len());

        let rows_to_show = if let Some(n) = limit {
            result.rows.iter().take(n).collect::<Vec<_>>()
        } else {
            result.rows.iter().collect::<Vec<_>>()
        };

        if !rows_to_show.is_empty() {
            println!("\nResults:");
            for (i, row) in rows_to_show.iter().enumerate() {
                if !json_output {
                    println!("\n--- Row {} ---", i + 1);
                }
                print_row(row, json_output)?;
            }
        }
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

fn parse_where_condition(s: &str) -> Option<KeyRange> {
    // Try different operators
    if let Some(pos) = s.find(">=") {
        let field = s[..pos].to_string();
        let value = parse_key_value(&s[pos + 2..]);
        return Some(KeyRange::gte(field, value));
    }
    if let Some(pos) = s.find("<=") {
        let field = s[..pos].to_string();
        let value = parse_key_value(&s[pos + 2..]);
        return Some(KeyRange::lte(field, value));
    }
    if let Some(pos) = s.find("gte") {
        let field = s[..pos].to_string();
        let value = parse_key_value(&s[pos + 3..]);
        return Some(KeyRange::gte(field, value));
    }
    if let Some(pos) = s.find("lte") {
        let field = s[..pos].to_string();
        let value = parse_key_value(&s[pos + 3..]);
        return Some(KeyRange::lte(field, value));
    }
    if let Some(pos) = s.find('>') {
        let field = s[..pos].to_string();
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange {
            field,
            start: hail_decoder::query::QueryBound::Excluded(value),
            end: hail_decoder::query::QueryBound::Unbounded,
        });
    }
    if let Some(pos) = s.find('<') {
        let field = s[..pos].to_string();
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange {
            field,
            start: hail_decoder::query::QueryBound::Unbounded,
            end: hail_decoder::query::QueryBound::Excluded(value),
        });
    }
    if let Some(pos) = s.find('=') {
        let field = s[..pos].to_string();
        let value = parse_key_value(&s[pos + 1..]);
        return Some(KeyRange::point(field, value));
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

fn convert(_input: &str, _output: &str) -> Result<()> {
    eprintln!("Conversion not yet implemented");
    Err(HailError::InvalidFormat("Not implemented".to_string()))
}

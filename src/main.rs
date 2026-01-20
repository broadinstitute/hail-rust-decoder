//! Hail decoder CLI tool

use hail_decoder::{HailError, Result};
use std::path::PathBuf;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <command> [args...]", args[0]);
        eprintln!("Commands:");
        eprintln!("  info <table.ht>         - Show table metadata");
        eprintln!("  convert <table.ht> <output.parquet> - Convert to Parquet");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "info" => {
            if args.len() < 3 {
                eprintln!("Usage: {} info <table.ht>", args[0]);
                std::process::exit(1);
            }
            show_info(&args[2]).await?;
        }
        "convert" => {
            if args.len() < 4 {
                eprintln!("Usage: {} convert <table.ht> <output.parquet>", args[0]);
                std::process::exit(1);
            }
            convert(&args[2], &args[3]).await?;
        }
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn show_info(table_path: &str) -> Result<()> {
    info!("Reading table metadata from: {}", table_path);

    let metadata_path = PathBuf::from(table_path)
        .join("metadata.json.gz");

    let data = tokio::fs::read(&metadata_path).await?;
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

async fn convert(_input: &str, _output: &str) -> Result<()> {
    eprintln!("Conversion not yet implemented");
    Err(HailError::InvalidFormat("Not implemented".to_string()))
}

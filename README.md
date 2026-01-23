# Hail Decoder

Pure Rust decoder for Hail table format with support for cloud storage, Parquet conversion, and database exports.

## Features

- **Zero Java/Hail dependencies**: Single static binary, no JVM required
- **Streaming capable**: Read from local disk or cloud storage (GCS, S3)
- **Memory efficient**: Process tables of any size with minimal memory
- **Multiple outputs**: Export to Parquet, VCF, ClickHouse, BigQuery
- **VCF support**: Query and export VCF files with tabix index support

## Installation

```bash
# Default build (GCS support)
cargo build --release

# Local files only (fastest compile)
cargo build --release --no-default-features

# Build with all features
cargo build --release --features full
```

## Quick Start

```bash
# View table metadata
hail-decoder info path/to/table.ht

# Query with filters
hail-decoder query path/to/table.ht --where ancestry=EUR --limit 10

# Export to Parquet
hail-decoder export parquet path/to/table.ht output.parquet

# Query cloud tables directly
hail-decoder info "gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht"
```

## Commands

### info

Show table metadata without scanning data (fast).

```bash
# Hail table
hail-decoder info data/variants.ht

# VCF file
hail-decoder info data/variants.vcf.bgz

# Cloud table
hail-decoder info "gs://bucket/path/to/table.ht"
```

### summary

Full scan to calculate row counts and field statistics.

```bash
hail-decoder summary data/analysis-meta.ht
```

### query

Stream rows with optional filtering.

```bash
# Basic query with limit
hail-decoder query data/table.ht --limit 10

# Filter by field value
hail-decoder query data/table.ht --where ancestry=EUR --limit 10

# Multiple filters
hail-decoder query data/table.ht --where ancestry=EUR --where trait_type=binary --limit 10

# Nested field filters
hail-decoder query data/table.ht --where "locus.contig=chr1" --where "locus.position>=55039447"

# JSON output
hail-decoder query data/table.ht --limit 5 --json

# Genomic interval filtering
hail-decoder query data/table.ht --interval "chr10:121500000-121600000" --limit 10

# Multiple intervals
hail-decoder query data/table.ht \
  --interval "chr10:121500000-121600000" \
  --interval "chr20:35400000-35500000" \
  --limit 10

# Intervals from file (BED, JSON, or text format)
hail-decoder query data/table.ht --intervals-file regions.bed --limit 10
```

### export parquet

Convert to Parquet format with optional filtering.

```bash
# Basic export
hail-decoder export parquet data/table.ht output.parquet

# With filters
hail-decoder export parquet data/table.ht output.parquet --where ancestry=EUR

# With interval filter
hail-decoder export parquet data/table.ht output.parquet --interval "chr10:121500000-121600000"

# Query with DuckDB
duckdb -c "SELECT * FROM 'output.parquet' LIMIT 5"
```

### export hail

Export to Hail table format (useful for subsetting).

```bash
hail-decoder export hail data/table.ht /tmp/subset.ht --interval "chr10:121500000-121600000"
```

### export vcf

Export to VCF format.

```bash
# Export with bgzip compression
hail-decoder export vcf data/variants.vcf.bgz output.vcf.gz --interval "chrX:31097677-31098000" --bgzip
```

### export clickhouse

Export to ClickHouse database (requires `--features clickhouse`).

```bash
hail-decoder export clickhouse \
  data/variants.ht \
  "http://user:pass@localhost:8123" \
  target_table \
  --intervals-file regions.bed

# Query in ClickHouse
curl -s "http://user:pass@localhost:8123" --data "SELECT * FROM target_table LIMIT 10 FORMAT Pretty"
```

### export bigquery

Export to BigQuery (requires `--features bigquery`).

```bash
hail-decoder export bigquery \
  data/variants.ht \
  project:dataset.table \
  --bucket staging-bucket \
  --intervals-file regions.bed

# Query in BigQuery
bq query --use_legacy_sql=false "SELECT * FROM dataset.table LIMIT 10"
```

### schema generate

Generate JSON schema from table.

```bash
# Print to stdout
hail-decoder schema generate data/table.ht

# Save to file
hail-decoder schema generate data/table.ht schema.json
```

### schema validate

Validate table data against JSON schema.

```bash
# Validate first N rows
hail-decoder schema validate data/table.ht schema.json --limit 100

# Validate random sample (faster for large tables)
hail-decoder schema validate data/table.ht schema.json --sample 1000

# Stop on first error
hail-decoder schema validate data/table.ht schema.json --fail-fast
```

## Feature Flags

| Feature | Description | Default |
|---------|-------------|---------|
| `gcp` | Google Cloud Storage support | Yes |
| `validation` | `schema validate` and `schema generate` commands | Yes |
| `aws` | Amazon S3 support | No |
| `http` | HTTP/HTTPS URL support | No |
| `clickhouse` | `export clickhouse` command | No |
| `bigquery` | `export bigquery` command (requires gcp) | No |
| `server` | `hail-server` HTTP binary | No |
| `full` | All features | No |

```bash
# Add S3 support
cargo build --release --features aws

# Full cloud support (GCS + S3 + HTTP)
cargo build --release --features gcp,aws,http

# Everything
cargo build --release --features full
```

## VCF Support

hail-decoder can read and query VCF files directly, with support for tabix indexing.

```bash
# View VCF metadata
hail-decoder info data/variants.vcf.bgz

# Query with interval (uses tabix index if available)
hail-decoder query data/variants.vcf.bgz --interval "chrX:31097677-31100000" --limit 10

# Generate schema from VCF
hail-decoder schema generate data/variants.vcf.bgz

# Validate VCF with sampling
hail-decoder schema validate data/variants.vcf.bgz schema.json --sample 10000
```

## Interval File Formats

The `--intervals-file` option supports multiple formats:

**BED format** (0-based, half-open):
```
chr1    55039446    55064852    PCSK9
chr2    178525988   178830802   TTN
```

**Text format** (1-based, inclusive):
```
chr1:55039447-55064852
chr2:178525989-178830802
```

**JSON format**:
```json
[
  {"contig": "chr1", "start": 55039447, "end": 55064852},
  {"contig": "chr2", "start": 178525989, "end": 178830802}
]
```

## Demo

See `examples/demo.sh` for a comprehensive walkthrough of all features.

## Testing

```bash
cargo test
cargo test --features full  # test all features
```

## Architecture

The decoder uses a unified `DataSource` abstraction that enables the same query interface across:
- Hail tables (`.ht` directories)
- VCF files (`.vcf`, `.vcf.gz`, `.vcf.bgz`)
- Local and cloud storage (GCS, S3)

All export commands share consistent `--where`, `--limit`, `--interval`, and `--intervals-file` options.

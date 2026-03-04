# Hail Decoder

Pure Rust decoder for Hail table format with support for cloud storage, Parquet conversion, and database exports.

## Features

- **Zero Java/Hail dependencies**: Single static binary, no JVM required
- **Local/Cloud Storage**: Read from local disk or cloud storage (GCS, S3)
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
genohype info path/to/table.ht

# Query with filters
genohype query path/to/table.ht --where ancestry=EUR --limit 10

# Export to Parquet
genohype export parquet path/to/table.ht output.parquet

# Query cloud tables directly
genohype info "gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht"
```

## Commands

### info

Show table metadata without scanning data (fast).

```bash
# Hail table
genohype info data/variants.ht

# VCF file
genohype info data/variants.vcf.bgz

# Cloud table
genohype info "gs://bucket/path/to/table.ht"
```

### summary

Full scan to calculate row counts and field statistics.

```bash
genohype summary data/analysis-meta.ht
```

### query

Stream rows with optional filtering.

```bash
# Basic query with limit
genohype query data/table.ht --limit 10

# Filter by field value
genohype query data/table.ht --where ancestry=EUR --limit 10

# Multiple filters
genohype query data/table.ht --where ancestry=EUR --where trait_type=binary --limit 10

# Nested field filters
genohype query data/table.ht --where "locus.contig=chr1" --where "locus.position>=55039447"

# JSON output
genohype query data/table.ht --limit 5 --json

# Genomic interval filtering
genohype query data/table.ht --interval "chr10:121500000-121600000" --limit 10

# Multiple intervals
genohype query data/table.ht \
  --interval "chr10:121500000-121600000" \
  --interval "chr20:35400000-35500000" \
  --limit 10

# Intervals from file (BED, JSON, or text format)
genohype query data/table.ht --intervals-file regions.bed --limit 10
```

### export parquet

Convert to Parquet format with optional filtering.

```bash
# Basic export
genohype export parquet data/table.ht output.parquet

# With filters
genohype export parquet data/table.ht output.parquet --where ancestry=EUR

# With interval filter
genohype export parquet data/table.ht output.parquet --interval "chr10:121500000-121600000"

# Query with DuckDB
duckdb -c "SELECT * FROM 'output.parquet' LIMIT 5"
```

### export hail

Export to Hail table format (useful for subsetting).

```bash
genohype export hail data/table.ht /tmp/subset.ht --interval "chr10:121500000-121600000"
```

### export vcf

Export to VCF format.

```bash
# Export with bgzip compression
genohype export vcf data/variants.vcf.bgz output.vcf.gz --interval "chrX:31097677-31098000" --bgzip
```

### export clickhouse

Export to ClickHouse database (requires `--features clickhouse`).

```bash
genohype export clickhouse \
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
genohype export bigquery \
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
genohype schema generate data/table.ht

# Save to file
genohype schema generate data/table.ht schema.json
```

### schema validate

Validate table data against JSON schema.

```bash
# Validate first N rows
genohype schema validate data/table.ht schema.json --limit 100

# Validate random sample (faster for large tables)
genohype schema validate data/table.ht schema.json --sample 1000

# Stop on first error
genohype schema validate data/table.ht schema.json --fail-fast
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

genohype can read and query VCF files directly, with support for tabix indexing.

```bash
# View VCF metadata
genohype info data/variants.vcf.bgz

# Query with interval (uses tabix index if available)
genohype query data/variants.vcf.bgz --interval "chrX:31097677-31100000" --limit 10

# Generate schema from VCF
genohype schema generate data/variants.vcf.bgz

# Validate VCF with sampling
genohype schema validate data/variants.vcf.bgz schema.json --sample 10000
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

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         HAIL-DECODER DATA FLOW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  INPUT SOURCES                  CORE ENGINE                 OUTPUT TARGETS  │
│  ─────────────                  ───────────                 ──────────────  │
│                                                                             │
│  ┌───────────┐                                              ┌───────────┐  │
│  │Hail Table │──┐                                        ┌─►│  stdout   │  │
│  │  (.ht)    │  │                                        │  │  (JSON)   │  │
│  └───────────┘  │                                        │  └───────────┘  │
│                 │    ┌─────────────────────────────┐     │                  │
│  ┌───────────┐  │    │        QueryEngine          │     │  ┌───────────┐  │
│  │ VCF File  │──┼───►│  ┌───────────────────────┐  │─────┼─►│  Parquet  │  │
│  │(.vcf.bgz) │  │    │  │   DataSource Trait    │  │     │  │ (.parquet)│  │
│  └───────────┘  │    │  │  - row_type()         │  │     │  └───────────┘  │
│                 │    │  │  - query_iter()       │  │     │                  │
│  ┌───────────┐  │    │  │  - key_fields()       │  │     │  ┌───────────┐  │
│  │  Remote   │──┘    │  └───────────────────────┘  │     ├─►│ClickHouse │  │
│  │(gs://,s3://)      │                             │     │  │  (HTTP)   │  │
│  └───────────┘       │  ┌───────────────────────┐  │     │  └───────────┘  │
│                      │  │   Index (optional)    │  │     │                  │
│                      │  │  - Partition bounds   │  │     │  ┌───────────┐  │
│                      │  │  - Tabix (VCF)        │  │     └─►│ BigQuery  │  │
│                      │  └───────────────────────┘  │        │(GCS+Load) │  │
│                      └─────────────────────────────┘        └───────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Principles:**
- **DataSource Abstraction** - Unified interface for Hail tables and VCF files
- **Streaming by Default** - Memory-efficient processing of arbitrarily large datasets
- **Parquet as Intermediate** - Bridge between row-oriented sources and columnar targets
- **Consistent CLI** - Same `--where`/`--limit`/`--interval` options work across all commands

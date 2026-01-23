# Hail Decoder

Pure Rust decoder for Hail table format with support for cloud storage, Parquet conversion, and database exports.

## Build

```bash
# Default build (GCS support)
cargo build

# Local files only (fastest compile, ~229 crates)
cargo build --no-default-features

# Build with all features (~408 crates)
cargo build --features full

# Build the HTTP server
cargo build --bin hail-server --features server
```

## Feature Flags

| Feature | Description | Default |
|---------|-------------|---------|
| `gcp` | Google Cloud Storage support | Yes |
| `aws` | Amazon S3 support | No |
| `http` | HTTP/HTTPS URL support | No |
| `validation` | `validate` and `generate-schema` commands | No |
| `clickhouse` | `export clickhouse` command | No |
| `bigquery` | `export bigquery` command (requires gcp) | No |
| `server` | `hail-server` HTTP binary | No |
| `full` | All features | No |

### Examples

```bash
# GCS + S3 support
cargo build --features aws

# CLI with validation commands
cargo build --features validation

# Full cloud support (GCS + S3 + HTTP)
cargo build --features gcp,aws,http

# Everything
cargo build --features full
```

## Commands

- `info` - Show basic table metadata
- `inspect` - Show detailed table information
- `summary` - Show comprehensive table summary with statistics
- `query` - Query with optional filters
- `convert` - Convert to Parquet format
- `validate` - Validate table against JSON schema (requires `validation` feature)
- `generate-schema` - Generate JSON schema from table (requires `validation` feature)
- `export clickhouse` - Export to ClickHouse (requires `clickhouse` feature)
- `export bigquery` - Export to BigQuery (requires `bigquery` feature)

## Test

```bash
cargo test
cargo test --features full  # test all features
```

Test VCFs and Hail tables can be found in `./data/`.

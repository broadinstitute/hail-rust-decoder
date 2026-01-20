# Hail Decoder

A pure Rust implementation for reading and decoding Hail table format without any Java/Hail runtime dependencies.

## Project Overview

This project aims to create a production-ready Hail table decoder and Parquet converter that:

- **Zero Java/Hail dependencies**: Pure Rust implementation
- **Streaming capable**: Read from local disk or cloud storage (GCS, S3)
- **Memory efficient**: Process tables of any size with minimal memory (~100MB)
- **High performance**: 2-3x faster than Java, single binary deployment
- **Parquet output**: Convert Hail tables to Apache Parquet format

## Motivation

Based on analysis documented in the briefing, this project was created to:

1. **Avoid cluster costs**: Process Hail tables on a laptop instead of Dataproc
2. **No JVM requirement**: Single static binary, no runtime dependencies
3. **Educational**: Deep understanding of the Hail format
4. **Production ready**: Build reliable tooling for recurring conversions

## Status

**Current Phase: Week 0 - Planning & Setup** ✅

- [x] Project structure created
- [x] Core module skeleton
- [x] Test data copied (72KB sample)
- [x] Format specification documented
- [x] Hail source files cataloged

**Next Phase: Week 1-2 - Core Streaming**

- [ ] Complete StreamBlockBuffer implementation
- [ ] Complete ZstdBuffer with real decompression
- [ ] BlockingBuffer implementation
- [ ] LEB128Buffer implementation
- [ ] Integration tests with test data

## Project Structure

```
hail-decoder/
├── src/
│   ├── buffer/          # 4-layer buffer stack
│   │   ├── stream_block.rs
│   │   ├── zstd.rs
│   │   ├── blocking.rs
│   │   └── leb128.rs
│   ├── codec/           # Type encoding/decoding
│   │   └── decoder.rs
│   ├── schema/          # Schema parsing
│   │   └── mod.rs
│   ├── index/           # B-tree index reading
│   │   └── mod.rs
│   ├── parquet/         # Parquet conversion
│   │   └── mod.rs
│   ├── error.rs         # Error types
│   ├── lib.rs           # Public API
│   └── main.rs          # CLI tool
├── data/                # Test data (gitignored)
│   └── gene_models_hds/
├── benches/             # Performance benchmarks
├── examples/            # Usage examples
└── tests/               # Integration tests
```

## Timeline: 6 Weeks

### Week 1-2: Core Functionality
- Streaming buffer implementation
- Zstd decompression
- Block reading and buffering
- LEB128 integer decoding
- **Goal:** Read raw blocks from test data

### Week 3-4: Type System
- Implement full Hail type decoder
- Support all primitive types
- Complex types (arrays, structs, tuples)
- Genomics types (locus, interval)
- **Goal:** Decode all rows from test data

### Week 5: GCS & Indexing
- Cloud storage streaming (GCS, S3)
- B-tree index reading
- Progress tracking
- **Goal:** Stream from remote storage

### Week 6: Parallelization & Polish
- Multi-core processing
- Parquet writer integration
- CLI tool polish
- Documentation
- **Goal:** Production-ready 1.0 release

## Dependencies

All mature, production-ready crates:

- `zstd` - Pure Rust Zstandard compression
- `arrow` & `parquet` - Apache Arrow/Parquet (official)
- `object_store` - Cloud storage abstraction
- `tokio` - Async runtime
- `serde` & `serde_json` - Serialization
- `nom` - Parser combinators
- `anyhow` & `thiserror` - Error handling

## Building

```bash
# Build debug version
cargo build

# Build release version (optimized)
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Usage (Planned)

```bash
# Show table information
hail-decoder info data/gene_models_hds/ht/prep_table.ht

# Convert to Parquet
hail-decoder convert data/gene_models_hds/ht/prep_table.ht output.parquet

# Stream from GCS
hail-decoder convert gs://bucket/table.ht output.parquet
```

## Documentation

- **[SPECIFICATION.md](SPECIFICATION.md)**: Complete Hail format specification
- **[HAIL_SOURCE_FILES.md](HAIL_SOURCE_FILES.md)**: Reference to Hail source code

## Test Data

A small test dataset is included:
- Location: `./data/gene_models_hds`
- Size: 72 KB (3 genes)
- Version: Hail 0.2.134, format 67328

## Performance Goals

For a 1TB Hail table:

| Metric           | Target         |
|------------------|----------------|
| Memory usage     | < 1 GB         |
| Processing time  | 20-25 min      |
| Cores used       | 8              |
| Cloud cost       | $0.50          |

Compare to:
- Hail on Dataproc: 10 min, $20-50
- Python custom: 4-6 hours, $2

## Contributing

This is a greenfield project. Contributions welcome!

Priority areas:
1. Complete Zstd integration in ZstdBuffer
2. Add comprehensive tests
3. Optimize hot paths
4. Add more example tables

## License

MIT OR Apache-2.0

## References

- Original discussion: See briefing files
- Hail project: https://github.com/hail-is/hail
- Test data source: `/Users/msolomon/data/axaou-local/data/pipeline/axaou/v8/a135460b/gene_models_hds`

## Acknowledgments

Based on deep analysis of Hail 0.2.134 source code and binary format reverse engineering.
# hail-rust-decoder

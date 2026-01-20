# Hail Decoder

A pure Rust implementation for reading and decoding Hail table format without any Java/Hail runtime dependencies.

## Project Overview

This project aims to create a Hail table decoder and Parquet converter that:

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

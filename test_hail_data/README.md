# Test Hail Tables for Decoder Development

This directory contains progressively complex Hail tables for testing the Rust decoder.

## Quick Start

Generate all test tables with one command:
```bash
./setup_test_tables.sh
```

This will:
1. Check for `uv` installation (install from https://github.com/astral-sh/uv if needed)
2. Create a Python virtual environment
3. Install Hail
4. Generate all 6 test tables

**Note**: The `.ht` directories are gitignored. Run the setup script after cloning the repo.

## Test Cases

### 01_single_int.py
**Simplest possible table**: Single row, single int32 field
- Tests: Basic table structure, single primitive type
- Expected: `value: 42`

### 02_primitives.py
**Multiple primitive types**: All basic types in one row (all required/non-nullable)
- Tests: int32, int64, float32, float64, bool, string
- Expected values: Various primitives with known values

### 03_simple_array.py
**Simple array**: Array of integers
- Tests: Array encoding, array length = 3
- Expected: `numbers: [1, 2, 3]`

### 04_simple_struct.py
**Simple struct**: Struct with required fields
- Tests: Struct encoding without nullability
- Expected: `info: {name: "BRCA1", length: 5000}`

### 05_interval_like.py
**Interval-like nested struct**: Replicates the gene table's interval structure
- Tests: Nested structs (interval -> start/end -> contig/position)
- Expected: chr10:100000-200000 with both bounds included

### 06_interval_plus_array.py ⭐ **KEY TEST**
**Interval + subsequent fields + array**: Replicates the exact structure causing issues
- Tests: Complex struct followed by primitives followed by array
- **This test replicates the bug scenario**: interval -> gene_id -> ... -> exons array
- Expected: 3 exons in the array
- **Use this to debug the byte 0x43 (-61) array length issue**

## Usage

### Install Hail with uv
```bash
cd test_hail_data
uv venv
source .venv/bin/activate  # or .venv/Scripts/activate on Windows
uv pip install hail
```

### Run all tests
```bash
./create_all_tables.sh
```

### Run individual test
```bash
python 01_single_int.py
```

## Output

Each script creates a corresponding `.ht` directory with:
- `rows/metadata.json.gz` - Contains the EType string and codec specification
- `rows/parts/part-*` - Binary data files to decode
- `.metadata.json.gz` - Additional metadata

## Debugging Strategy

1. Start with `01_single_int` - verify basic decoding works
2. Progress through 02-05 to verify each feature
3. Focus on `06_interval_plus_array` - this should reproduce the current bug
4. Compare byte-by-byte with working tests to find the misalignment

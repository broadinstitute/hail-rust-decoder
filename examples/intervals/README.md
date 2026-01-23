# Test Gene Intervals

Curated genomic intervals for 10 well-known genes, sourced from the [gnomAD API](https://gnomad.broadinstitute.org/) (GRCh38 reference genome).

## Files

All three files contain the same intervals in different formats:

| File | Format | Coordinates |
|------|--------|-------------|
| `test_genes.json` | JSON array | 1-based, inclusive |
| `test_genes.bed` | BED | 0-based, half-open |
| `test_genes.txt` | Text | 1-based, inclusive |

## Included Genes

| Gene | Chromosome | Size | Description |
|------|------------|------|-------------|
| PCSK9 | chr1 | 25 kb | Cholesterol metabolism |
| TTN | chr2 | 305 kb | Titin (largest human gene) |
| EGFR | chr7 | 193 kb | Cancer therapeutic target |
| CFTR | chr7 | 429 kb | Cystic fibrosis |
| HBB | chr11 | 4 kb | Hemoglobin beta (small gene) |
| BRCA2 | chr13 | 85 kb | Breast cancer susceptibility |
| TP53 | chr17 | 26 kb | Tumor suppressor |
| BRCA1 | chr17 | 126 kb | Breast cancer susceptibility |
| APOE | chr19 | 4 kb | Alzheimer's/lipid metabolism |
| DMD | chrX | 2.2 Mb | Dystrophin (X-linked) |

## Usage

Filter queries or exports to these test genes:

```bash
# Query with JSON intervals
hail-decoder query table.ht --intervals-file examples/intervals/test_genes.json

# Query with BED intervals
hail-decoder query table.ht --intervals-file examples/intervals/test_genes.bed

# Query with text intervals
hail-decoder query table.ht --intervals-file examples/intervals/test_genes.txt

# Export filtered to Parquet
hail-decoder export parquet table.ht output.parquet --intervals-file examples/intervals/test_genes.bed

# Combine with CLI intervals
hail-decoder query table.ht --intervals-file examples/intervals/test_genes.txt --interval "chr3:100-200"
```

## Format Details

### JSON Format

```json
[
  {"contig": "chr1", "start": 55039447, "end": 55064852, "gene": "PCSK9", "gene_id": "ENSG00000169174"},
  ...
]
```

Supports `contig`, `chrom`, or `chr` for the chromosome field. Extra fields (like `gene`, `gene_id`) are ignored.

### BED Format

```
chr1	55039446	55064852	PCSK9	.	+
```

Standard BED format with 0-based start, half-open end. Optional columns 4-6 for name, score, strand.

### Text Format

```
chr1:55039447-55064852
```

Simple `chr:start-end` format, one interval per line. Comments start with `#`.

#!/bin/bash
# hail-decoder Demo Script
# Run commands one at a time in a tmux split

# Build release binary
cargo build --release --bin hail-decoder

# ============================================
# INFO - Show table metadata (fast, no scan)
# ============================================

# View metadata for analysis table
hail-decoder info data/analysis-meta.ht

# View metadata for variant table with locus field
hail-decoder info data/variants.ht

# ============================================
# SUMMARY - Full scan with statistics (slower)
# ============================================

# Get row count and field statistics
hail-decoder summary data/analysis-meta.ht

# ============================================
# QUERY - Stream rows with filtering
# ============================================

# Basic query with limit
hail-decoder query data/analysis-meta.ht --limit 3

# Filter by field value
hail-decoder query data/analysis-meta.ht --where ancestry=EUR --limit 3

# Multiple filters combined
hail-decoder query data/analysis-meta.ht --where ancestry=EUR --where trait_type=binary --limit 3

# Query with JSON output
hail-decoder query data/analysis-meta.ht --where ancestry=AFR --limit 2 --json

# ============================================
# GCS - Query from Google Cloud Storage
# ============================================

# Query public gnomAD table (requires GCS auth)
hail-decoder info "gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht"

# Query with nested field filters
hail-decoder query "gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht" \
  --where "locus.contig=chr1" \
  --where "locus.position>=55039447" \
  --where "locus.position<=55064852" \
  --limit 3

# ============================================
# QUERY - Genomic interval filtering
# ============================================

# Single interval
hail-decoder query data/variants.ht --interval "chr10:121500000-121600000" --limit 5

# Filter by ancestry within interval
hail-decoder query data/variants.ht --interval "chr10:121500000-121600000" --where ancestry_group=eur --limit 3

# Multiple intervals
hail-decoder query data/variants.ht \
  --interval "chr10:121500000-121600000" \
  --interval "chr20:35400000-35500000" \
  --limit 5

# ============================================
# EXPORT - Convert to other formats
# ============================================

# Export to Parquet with filters
hail-decoder export parquet \
  data/analysis-meta.ht \
  /tmp/analysis-eur.parquet \
  --where ancestry=EUR

# Query the parquet file with DuckDB
duckdb -c "SELECT phenoname, ancestry, n_cases, n_controls FROM '/tmp/analysis-eur.parquet' LIMIT 5"

# Export subset to new Hail table
hail-decoder export hail \
  data/variants.ht \
  /tmp/variants-chr10.ht \
  --interval "chr10:121500000-121600000"

# Verify exported table with Hail
uv run --with hail python -c "import hail as hl; hl.init(quiet=True); ht = hl.read_table('/tmp/variants-chr10.ht'); ht.describe()"

# Export filtered VCF with bgzip compression
hail-decoder export vcf \
  "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz" \
  /tmp/filtered.vcf.gz \
  --interval "chrX:31097677-31098000" \
  --bgzip

# Inspect the exported VCF
gzcat /tmp/filtered.vcf.gz | head -30

# ============================================
# SCHEMA - Generate and validate schemas
# ============================================

# Generate JSON schema from table
hail-decoder schema generate data/analysis-meta.ht

# Save schema to file
hail-decoder schema generate data/analysis-meta.ht /tmp/analysis-schema.json

# Validate table against schema
hail-decoder schema validate data/analysis-meta.ht /tmp/analysis-schema.json

# Validation failure (wrong schema for table)
hail-decoder schema validate data/variants.ht /tmp/analysis-schema.json --limit 3

# Validate cloud table with random sampling
hail-decoder schema generate "gs://axaou-central/ms/axaou/v8/515c3dc3/genome_variant_annotations_hds/ht/prep_table.ht" /tmp/cloud-schema.json

hail-decoder schema validate "gs://axaou-central/ms/axaou/v8/515c3dc3/genome_variant_annotations_hds/ht/prep_table.ht" /tmp/cloud-schema.json --sample 100

# VCF info (works on VCF files too)
hail-decoder info "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz"

# Validate VCF
hail-decoder schema generate "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz" /tmp/vcf-schema.json

hail-decoder schema validate "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz" /tmp/vcf-schema.json --sample 10000

# Query VCF with interval using tabix index (~2kb region in DMD gene)
hail-decoder query "./data/gnomad.exomes.v4.1.sites.chrX.vcf.bgz" --interval "chrX:31097677-31100000" 

# ============================================
# CLICKHOUSE - Export to ClickHouse database
# ============================================

# Export variants using intervals file (requires --features clickhouse)
hail-decoder export clickhouse \
  data/variants.ht \
  "http://default:test@localhost:8123" \
  demo_variants \
  --intervals-file data/demo_intervals.txt

# Query the exported data
curl -s "http://default:test@localhost:8123" --data "SELECT locus.1 as contig, locus.2 as position, ref, alt, ancestry_group, allele_frequency FROM demo_variants LIMIT 10 FORMAT Pretty"

# Aggregate by ancestry
curl -s "http://default:test@localhost:8123" --data "SELECT ancestry_group, count() as variants, round(avg(allele_frequency), 6) as avg_af FROM demo_variants GROUP BY ancestry_group FORMAT Pretty"

# ============================================
# BIGQUERY - Export to BigQuery (requires --features bigquery)
# ============================================

# Export with interval filter to BigQuery
hail-decoder export bigquery \
  data/variants.ht \
  aou-neale-gwas-browser:hail_test.demo_variants \
  --bucket axaou-central-tmp \
  --intervals-file data/demo_intervals.txt

# Query the exported data in BigQuery
bq query --use_legacy_sql=false "SELECT locus.contig, locus.position, ref, alt, ancestry_group, allele_frequency FROM hail_test.demo_variants LIMIT 10"

# ============================================
# Cleanup
# ============================================

rm -f /tmp/analysis-eur.parquet /tmp/filtered.vcf.gz
rm -rf /tmp/variants-chr10.ht /tmp/analysis-schema.json

"""
Test 6: Interval-like structure followed by more fields and an array
This replicates the exact structure causing issues:
interval -> gene_id -> ... -> exons (array)
"""
import hail as hl

hl.init(quiet=True)

data = [{
    "interval": {
        "start": {"contig": "chr10", "position": 121478332},
        "end": {"contig": "chr10", "position": 121598458},
        "includes_start": True,
        "includes_end": True,
    },
    "gene_id": "ENSG00000066468",
    "gene_version": "24",
    "symbol": "FGFR2",
    "chrom": "10",
    "strand": "-",
    "start": 121478332,
    "stop": 121598458,
    "xstart": 10121478332,
    "xstop": 10121598458,
    "exons": [
        {"type": "CDS", "start": 121478332, "end": 121500000},
        {"type": "CDS", "start": 121510000, "end": 121530000},
        {"type": "CDS", "start": 121540000, "end": 121598458},
    ],
}]

ht = hl.Table.parallelize(data)
output_path = "06_interval_plus_array.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")
print(f"  Exons array length: {len(ht.collect()[0]['exons'])}")

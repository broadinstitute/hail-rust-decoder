"""
Test 5: Interval-like structure (nested structs like in gene table)
Mimics: interval -> start/end -> contig/position
"""
import hail as hl

hl.init(quiet=True)

data = [{
    "interval": {
        "start": {
            "contig": "chr10",
            "position": 100000,
        },
        "end": {
            "contig": "chr10",
            "position": 200000,
        },
        "includes_start": True,
        "includes_end": True,
    }
}]

# Explicitly define the type to match gene table structure
data_type = """struct{
    interval: struct{
        start: struct{contig: str, position: int32},
        end: struct{contig: str, position: int32},
        includes_start: bool,
        includes_end: bool
    }
}"""

ht = hl.Table.parallelize(hl.literal(data, data_type))
output_path = "05_interval_like.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")

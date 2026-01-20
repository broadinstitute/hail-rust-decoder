"""
Test 1: Simplest possible table - single row, single int32 field
"""
import hail as hl

hl.init(quiet=True)

data = [{"value": 42}]
ht = hl.Table.parallelize(data)
output_path = "test_hail_data/01_single_int.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")

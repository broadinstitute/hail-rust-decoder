"""
Test 3: Simple array of integers
"""
import hail as hl

hl.init(quiet=True)

data = [{
    "id": "test1",
    "numbers": [1, 2, 3],
}]
ht = hl.Table.parallelize(data)
output_path = "test_hail_data/03_simple_array.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")
print(f"  Array length: {ht.collect()[0]['numbers'].__len__()}")

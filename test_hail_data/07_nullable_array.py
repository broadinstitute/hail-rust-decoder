"""
Test 7: Simple structure with NULLABLE array
This tests if nullable arrays are encoded differently than required arrays.
By including None in one row, we make the field nullable in the schema.
"""
import hail as hl

hl.init(quiet=True)

# Create data with one row having the array, one having None
# This forces Hail to make the array field nullable
data = [
    {"id": "test1", "numbers": [1, 2, 3]},
    {"id": "test2", "numbers": None},  # This makes 'numbers' nullable
]

ht = hl.Table.parallelize(data)
output_path = "07_nullable_array.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")
print(f"  First row array length: {len(ht.collect()[0]['numbers'])}")

"""
Test 2: Multiple primitive types (all required/non-nullable)
"""
import hail as hl

hl.init(quiet=True)

data = [{
    "int32_val": 42,
    "int64_val": 1234567890,
    "float32_val": 3.14,
    "float64_val": 2.71828,
    "bool_val": True,
    "str_val": "hello",
}]
ht = hl.Table.parallelize(data)
output_path = "02_primitives.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")

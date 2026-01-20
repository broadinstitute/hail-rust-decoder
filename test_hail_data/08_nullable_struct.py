"""
Test 8: Nullable struct field
This tests if nullable structs have present flags even when bitmap says present.
"""
import hail as hl

hl.init(quiet=True)

# Create data with nullable struct
# Force nullable by including a None value in one row
data = [
    {
        "id": "test1",
        "info": {"name": "Alice", "age": 30},
        "value": 100
    },
    {
        "id": "test2",
        "info": None,  # Makes 'info' nullable
        "value": 200
    },
]

ht = hl.Table.parallelize(data)
output_path = "08_nullable_struct.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")
print(f"  First row info: {ht.collect()[0]['info']}")

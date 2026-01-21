"""
Test 9: Simple Set
Tests decoding of TSet encoded as EUnsortedSet (which is just an EArray internally)
"""
import hail as hl

hl.init(quiet=True)

# Create data with sets
data = [
    {
        "id": "test1",
        "tags": hl.set([1, 2, 3]),
        "value": 100
    },
    {
        "id": "test2",
        "tags": hl.set([4, 5]),
        "value": 200
    },
    {
        "id": "test3",
        "tags": hl.set([6]),  # Single element
        "value": 300
    },
]

ht = hl.Table.parallelize(data)
output_path = "09_simple_set.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")
print(f"  First row tags: {ht.collect()[0]['tags']}")
print(f"  Second row tags: {ht.collect()[1]['tags']}")

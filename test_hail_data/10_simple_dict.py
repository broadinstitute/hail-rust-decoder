"""
Test 10: Simple Dict
Tests decoding of TDict encoded as EDictAsUnsortedArrayOfPairs
(which is an EArray of EBaseStruct with key/value fields)
"""
import hail as hl

hl.init(quiet=True)

# Create data with dicts
data = [
    {
        "id": "test1",
        "metadata": {"name": "Alice", "age": "30", "city": "NYC"},
        "value": 100
    },
    {
        "id": "test2",
        "metadata": {"name": "Bob", "age": "25"},
        "value": 200
    },
    {
        "id": "test3",
        "metadata": {"status": "active"},  # Single entry
        "value": 300
    },
]

ht = hl.Table.parallelize(data)
output_path = "10_simple_dict.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")
print(f"  First row metadata: {ht.collect()[0]['metadata']}")
print(f"  Second row metadata: {ht.collect()[1]['metadata']}")

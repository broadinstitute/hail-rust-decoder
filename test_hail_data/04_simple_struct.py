"""
Test 4: Simple struct with required fields
"""
import hail as hl

hl.init(quiet=True)

data = [{
    "id": "gene1",
    "info": {
        "name": "BRCA1",
        "length": 5000,
    }
}]
ht = hl.Table.parallelize(data)
output_path = "04_simple_struct.ht"
ht.write(output_path, overwrite=True)

print(f"✓ Created: {output_path}")
print(f"  Schema: {ht.row.dtype}")
print(f"  Rows: {ht.count()}")

#!/bin/bash
# Create all test Hail tables

set -e

echo "Creating test Hail tables..."
echo

for script in 0*.py; do
    echo "Running $script..."
    python "$script"
    echo
done

echo "✓ All test tables created!"
echo
echo "Generated tables:"
ls -d *.ht 2>/dev/null || echo "  (none yet - run the scripts first)"

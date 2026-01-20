#!/bin/bash
# Setup script for generating Hail test tables
# Run this script to create all test tables needed for decoder testing

set -e  # Exit on error

echo "=========================================="
echo "Hail Test Table Generator"
echo "=========================================="
echo

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ Error: 'uv' is not installed"
    echo
    echo "Please install uv first:"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo
    echo "Or visit: https://github.com/astral-sh/uv"
    exit 1
fi

echo "✓ Found uv: $(uv --version)"
echo

# Navigate to test_hail_data directory
cd "$(dirname "$0")"

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating Python virtual environment..."
    uv venv
    echo "✓ Virtual environment created"
    echo
fi

# Install Hail
echo "Installing Hail (this may take a minute)..."
uv pip install hail --quiet
echo "✓ Hail installed"
echo

# Generate test tables
echo "=========================================="
echo "Generating Test Tables"
echo "=========================================="
echo

test_scripts=(
    "01_single_int.py"
    "02_primitives.py"
    "03_simple_array.py"
    "04_simple_struct.py"
    "05_interval_like.py"
    "06_interval_plus_array.py"
)

for script in "${test_scripts[@]}"; do
    if [ -f "$script" ]; then
        echo "Running $script..."
        uv run python "$script" 2>&1 | grep "✓\|Created\|Schema\|Rows\|Array length\|Exons"
        echo
    else
        echo "⚠️  Warning: $script not found, skipping"
        echo
    fi
done

echo "=========================================="
echo "Summary"
echo "=========================================="
echo

# List generated tables
if ls *.ht &> /dev/null; then
    echo "✓ Generated test tables:"
    for dir in *.ht; do
        if [ -d "$dir" ]; then
            size=$(du -sh "$dir" | cut -f1)
            echo "  - $dir ($size)"
        fi
    done
    echo
    echo "✓ All test tables generated successfully!"
    echo
    echo "You can now run the Rust tests:"
    echo "  cargo test"
else
    echo "❌ No test tables were generated"
    echo "Please check the error messages above"
    exit 1
fi

echo
echo "To regenerate tables, run: $0"

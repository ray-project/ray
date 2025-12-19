#!/bin/bash

# Script to regenerate README.md files from Jupyter notebooks
# This ensures all markdown files are in sync with their source notebooks

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Regenerating README.md files from notebooks..."

# Array of directories containing notebooks
NOTEBOOK_DIRS=(
    "distributing-pytorch"
    "pytorch-profiling"
    "pytorch-fsdp"
    "deepspeed_finetune"
)

# Convert each notebook to markdown
for dir in "${NOTEBOOK_DIRS[@]}"; do
    NOTEBOOK_PATH="$SCRIPT_DIR/$dir/README.ipynb"
    if [ -f "$NOTEBOOK_PATH" ]; then
        echo "Converting $dir/README.ipynb to README.md..."
        jupyter nbconvert --to markdown "$NOTEBOOK_PATH" --output README.md
        echo "  ✓ $dir/README.md regenerated"
    else
        echo "  ✗ Warning: $NOTEBOOK_PATH not found"
    fi
done

echo ""
echo "All README.md files have been regenerated!"


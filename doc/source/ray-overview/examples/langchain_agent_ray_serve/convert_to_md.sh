#!/bin/bash

# Convert README.ipynb to README.md for template preview
# Run this script whenever you update the notebook to keep README.md in sync

set -euxo pipefail

jupyter nbconvert content/README.ipynb --to markdown --output README.md

echo "Successfully converted content/README.ipynb to content/README.md"

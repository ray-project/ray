#!/bin/bash

set -exo pipefail

# Delete README.md if it already exists
[ -f content/README.md ] && rm content/README.md

# Convert notebook to Markdown
jupyter nbconvert content/README.ipynb --to markdown --output README.md --output-dir content

# Prepend warning comment (hidden in rendered docs)
tmp_file="$(mktemp)"
{
  echo "<!--"
  echo "Do not modify this README. This file is a copy of the notebook and is not used to display the content."
  echo "Modify README.ipynb instead, then regenerate this file with:"
  echo "jupyter nbconvert \"content/README.ipynb\" --to markdown --output \"README.md\""
  echo "Or use this script: bash convert_to_md.sh"
  echo "-->"
  echo ""
  cat content/README.md
} > "$tmp_file"
mv "$tmp_file" content/README.md

echo "Successfully converted content/README.ipynb to content/README.md"

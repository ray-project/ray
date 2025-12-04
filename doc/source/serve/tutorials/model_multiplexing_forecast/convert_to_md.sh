#!/bin/bash

set -exo pipefail

nb="content/notebook"
md_dir="$(dirname "$nb")"
md_file="$md_dir/README.md"

# Delete README if it already exists
[ -f "$md_file" ] && rm "$md_file"

# Convert notebook to Markdown
jupyter nbconvert "${nb}.ipynb" --to markdown --output "README.md" --output-dir "$md_dir"

# Prepend warning comment (always, hidden in rendered docs)
tmp_file="$(mktemp)"
{
  echo "<!--"
  echo "Do not modify this README. This file is a copy of the notebook and is not used to display the content."
  echo "Modify notebook.ipynb instead, then regenerate this file with:"
  echo "jupyter nbconvert \"content/notebook.ipynb\" --to markdown --output \"README.md\""
  echo "Or use this script: bash convert_to_md.sh"
  echo "-->"
  echo ""
  cat "$md_file"
} > "$tmp_file"
mv "$tmp_file" "$md_file"

# Prepend orphan header
tmp_file="$(mktemp)"
{
  echo "---"
  echo "orphan: true"
  echo "---"
  echo ""
  cat "$md_file"
} > "$tmp_file"
mv "$tmp_file" "$md_file"

echo "✅ Successfully converted content/notebook.ipynb to content/README.md"


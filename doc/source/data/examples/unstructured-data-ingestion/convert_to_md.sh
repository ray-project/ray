#!/bin/bash

set -exo pipefail

nb_rel_path="content/unstructured-data-ingestion.ipynb"
content_dir="$(dirname "$nb_rel_path")"
nb_filename="$(basename "$nb_rel_path")"
md_rel_path="$content_dir/README.md"

# Delete README if it already exists
[ -f "$md_rel_path" ] && rm "$md_rel_path"
    
# Convert notebook to Markdown
jupyter nbconvert "$nb_rel_path" --to markdown --output README.md

# Prepend warning comment (will be hidden when rendered in the console)
tmp_file="$(mktemp)"
{
echo "<!--"
echo "Do not modify this README. This file is a copy of the notebook and is not used to display the content."
echo "Modify $nb_filename instead, then regenerate this file with:"
echo "jupyter nbconvert \"\$nb_filename\" --to markdown --output \"README.md\""
echo "-->"
echo ""
cat "$md_rel_path"
} > "$tmp_file"
mv "$tmp_file" "$md_rel_path"

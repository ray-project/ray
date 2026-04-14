#!/bin/bash

set -exo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Subdirectories under content/ containing README.ipynb -> README.md
subdirs=(
  gpt-oss
  hybrid-reasoning-llm
  large-size-llm
  medium-size-llm
  reasoning-llm
  small-size-llm
  vision-llm
)

convert_notebook() {
  local nb_path="$1"   # e.g. content/gpt-oss/README or content/README
  local nb_dir="$(dirname "$nb_path")"
  local nb_base="$(basename "$nb_path")"
  local md_file="$nb_dir/README.md"

  # Delete README.md if it already exists
  [ -f "$md_file" ] && rm "$md_file"

  # Convert notebook to Markdown
  jupyter nbconvert "${nb_path}.ipynb" --to markdown --output "README.md" --output-dir "$nb_dir"

  # Prepend warning comment
  tmp_file="$(mktemp)"
  {
    echo "<!--"
    echo "Do not modify this README. This file is a copy of the notebook and is not used to display the content."
    echo "Modify ${nb_base}.ipynb instead, then regenerate this file with:"
    echo "jupyter nbconvert \"${nb_path}.ipynb\" --to markdown --output \"README.md\""
    echo "Or use this script: bash convert_to_md.sh"
    echo "-->"
    echo ""
    cat "$md_file"
  } > "$tmp_file"
  mv "$tmp_file" "$md_file"
}

cd "$SCRIPT_DIR"

# Convert each subdirectory's README.ipynb -> README.md
for subdir in "${subdirs[@]}"; do
  convert_notebook "content/${subdir}/README"
  echo "✅ Successfully converted content/${subdir}/README.ipynb to content/${subdir}/README.md"
done

#!/bin/bash

set -exo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Subdirectories containing README.ipynb -> README.md
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
  local nb_path="$1"   # e.g. gpt-oss/README or README
  local nb_dir="$(dirname "$nb_path")"
  local nb_base="$(basename "$nb_path")"
  local md_file="$nb_dir/README.md"
  local add_orphan="$2" # "true" or "false"

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

  # Optionally prepend orphan header
  if [ "$add_orphan" = "true" ]; then
    tmp_file="$(mktemp)"
    {
      echo "---"
      echo "orphan: true"
      echo "---"
      echo ""
      cat "$md_file"
    } > "$tmp_file"
    mv "$tmp_file" "$md_file"
  fi
}

cd "$SCRIPT_DIR"

# Convert top-level README.ipynb -> README.md (no orphan header)
convert_notebook "README" "false"
echo "✅ Successfully converted README.ipynb to README.md"

# Convert each subdirectory's README.ipynb -> README.md (with orphan header)
for subdir in "${subdirs[@]}"; do
  convert_notebook "${subdir}/README" "true"
  echo "✅ Successfully converted ${subdir}/README.ipynb to ${subdir}/README.md"
done

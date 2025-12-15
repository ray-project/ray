#!/bin/bash

set -exo pipefail

for nb_dir in \
  "content" \
  "content/small-size-llm" \
  "content/medium-size-llm" \
  "content/large-size-llm" \
  "content/vision-llm" \
  "content/reasoning-llm" \
  "content/hybrid-reasoning-llm" \
  "content/gpt-oss"
do
  # The directory where we want to output the README.md
  md_file="$nb_dir/README.md"

  # Delete README if it already exists
  [ -f "$md_file" ] && rm "$md_file"

  # Convert notebook to Markdown
  jupyter nbconvert "${nb_dir}/README.ipynb" --to markdown --output "README.md"

  # Prepend warning comment (always, hidden in rendered docs in the console)
  tmp_file="$(mktemp)"
  {
    echo "<!--"
    echo "Do not modify this README. This file is a copy of the notebook and is not used to display the content."
    echo "Modify notebook.ipynb instead, then regenerate this file with:"
    echo "jupyter nbconvert \"README.ipynb\" --to markdown --output \"README.md\""
    echo "-->"
    echo ""
    cat "$md_file"
  } > "$tmp_file"
  mv "$tmp_file" "$md_file"

done

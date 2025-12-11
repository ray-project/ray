#!/bin/bash

set -exo pipefail

# Function to convert notebook to README.md with warning comment
convert_nb_to_readme() {
    local nb_path="$1"
    local output_dir="$2"
    local nb_filename="$(basename "$nb_path")"
    local md_path="$output_dir/README.md"
    
    # Delete README if it already exists
    [ -f "$md_path" ] && rm "$md_path"
    
    # Convert notebook to Markdown
    jupyter nbconvert "$nb_path" --to markdown --output README.md --output-dir "$output_dir"
    
    # Prepend warning comment (will be hidden when rendered in the console)
    tmp_file="$(mktemp)"
    {
        echo "<!--"
        echo "Do not modify this README. This file is a copy of the notebook and is not used to display the content."
        echo "Modify $nb_filename instead, then regenerate this file with:"
        echo "jupyter nbconvert \"$nb_filename\" --to markdown --output \"README.md\""
        echo "-->"
        echo ""
        cat "$md_path"
    } > "$tmp_file"
    mv "$tmp_file" "$md_path"
    
    echo "Generated $md_path from $nb_filename"
}

convert_nb_to_readme "content/README.ipynb" "content"

# Convert getting-started: Use the single notebook as source
convert_nb_to_readme "content/getting-started/01_02_03_intro_to_ray_train.ipynb" "content/getting-started"

# Convert workload-patterns: Use the overview README.ipynb
convert_nb_to_readme "content/workload-patterns/README.ipynb" "content/workload-patterns"

# Convert hyperparameter-tuning: Use the single notebook as source
convert_nb_to_readme "content/hyperparameter-tuning/05a_ray_tune_pytorch_example.ipynb" "content/hyperparameter-tuning"

echo "All README.md files generated successfully!"


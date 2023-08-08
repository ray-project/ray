#!/bin/bash


# Exit if any of the test commands fail.
set -e pipeline

INPUT_DIR=~/imagenet-1gb
OUTPUT_DIR=~/imagenet-1gb-data

# Download 1GB dataset from S3 to local disk.
aws s3 sync s3://air-cuj-imagenet-1gb $INPUT_DIR

# Preprocess files to get to the directory structure that torch dataloader
# expects.
for filename in "$INPUT_DIR"/*; do
    filename=$(basename "$filename")
    class_dir=$(echo "$filename" | awk '{split($0, array, "_"); print array[1]}')
    img_path=$(echo "$filename" | awk '{split($0, array, "_"); print array[2]}')
    mkdir -p "$OUTPUT_DIR"/"$class_dir"
    out_path="$OUTPUT_DIR/$class_dir/$img_path"
    echo "$out_path"
    cp "$INPUT_DIR"/"$filename" "$out_path"
done

python image_loader_microbenchmark.py

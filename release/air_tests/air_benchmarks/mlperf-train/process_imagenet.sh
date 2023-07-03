#!/bin/bash

for filename in `ls $INPUT_DIR`; do
    class_dir=$(echo $filename | awk '{split($0, array, "_"); print array[1]}')
    img_path=$(echo $filename | awk '{split($0, array, "_"); print array[2]}')
    mkdir -p $OUTPUT_DIR/$class_dir
    out_path="$OUTPUT_DIR/$class_dir/$img_path"
    echo $out_path
    cp $INPUT_DIR/$filename $out_path
done
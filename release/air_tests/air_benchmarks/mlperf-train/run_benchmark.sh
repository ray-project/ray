#!/bin/bash

for i in 1 4 8; do
    for j in 512; do
        NUM_IMAGES_PER_FILE=$j NUM_FILES=$(( i * 512 / j)) NUM_EPOCHS=3 bash file_size_benchmark.sh
    done
done

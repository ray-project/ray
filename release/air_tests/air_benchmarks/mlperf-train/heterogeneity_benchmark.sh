#!/bin/bash
# Test Ray Data bulk ingestion performance as the number of CPU nodes changes.

# Exit if any of the test commands fail.
set -x -e

NUM_FILES="128"
NUM_IMAGES_PER_FILE="64 512"
NUM_CPU_NODES=${1:-2}

NUM_EPOCHS=1
BATCH_SIZE=64
SHUFFLE_BUFFER_SIZE=0
DATA_DIR="/home/ray/data"


for num_images_per_file in $NUM_IMAGES_PER_FILE; do
    # Wait for all nodes to start and generate data files.
    python make_fake_dataset.py \
        --num-nodes $((NUM_CPU_NODES + 1)) \
        --num-shards $NUM_FILES \
        --num-images-per-shard "$num_images_per_file" \
        --output-directory $DATA_DIR

    python resnet50_ray_air.py \
        --num-images-per-input-file "$num_images_per_file" \
        --num-epochs $NUM_EPOCHS \
        --batch-size $BATCH_SIZE \
        --shuffle-buffer-size $SHUFFLE_BUFFER_SIZE \
        --num-images-per-epoch $(( num_images_per_file * NUM_FILES )) \
        --num-cpu-nodes "$NUM_CPU_NODES" \
        --train-sleep-time-ms 0 \
        --data-root $DATA_DIR \
        --use-ray-data
    sleep 5
done

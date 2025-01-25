#!/bin/bash
# Test Ray Data vs. tf.data bulk ingestion performance as the size of input
# files changes.

# Exit if any of the test commands fail.
set -x -e pipeline

BATCH_SIZE=32
SHUFFLE_BUFFER_SIZE=0
DATA_DIR=/home/ray/data
SHARD_URL_PREFIX=https://air-example-data.s3.us-west-2.amazonaws.com/air-benchmarks

NUM_EPOCHS=${NUM_EPOCHS:-"1"}

run() {
    NUM_FILES=$((NUM_IMAGES_PER_EPOCH / NUM_IMAGES_PER_FILE))
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    time python make_fake_dataset.py \
        --num-shards "$NUM_FILES" \
        --shard-url "$SHARD_URL_PREFIX/single-image-repeated-$NUM_IMAGES_PER_FILE-times" \
        --output-directory "$DATA_DIR"

    time python resnet50_ray_air.py \
        --num-images-per-input-file "$NUM_IMAGES_PER_FILE" \
        --num-epochs "$NUM_EPOCHS" \
        --batch-size "$BATCH_SIZE" \
        --shuffle-buffer-size "$SHUFFLE_BUFFER_SIZE" \
        --num-images-per-epoch "$NUM_IMAGES_PER_EPOCH" \
        --train-sleep-time-ms 0 \
        --data-root "$DATA_DIR" \
        --use-ray-data 2>&1 | tee -a out
}

# Many files to make sure we don't OOM.
NUM_IMAGES_PER_FILE=8192
NUM_IMAGES_PER_EPOCH=$(( 8192 * 10 ))
run

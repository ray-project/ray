#!/bin/bash
# Test Ray Data vs. tf.data bulk ingestion performance as the size of input
# files changes.

# Exit if any of the test commands fail.
set -x -e pipeline

BATCH_SIZE=32
SHUFFLE_BUFFER_SIZE=0
DATA_DIR=/home/ray/data
SHARD_URL_PREFIX=https://air-example-data.s3.us-west-2.amazonaws.com/air-benchmarks

# First epoch is for warmup, report results from second.
NUM_EPOCHS=${NUM_EPOCHS:-"2"}

run() {
    NUM_FILES=$((NUM_IMAGES_PER_EPOCH / NUM_IMAGES_PER_FILE))
    rm -rf $DATA_DIR
    mkdir -p $DATA_DIR
    python make_fake_dataset.py \
        --num-shards "$NUM_FILES" \
        --shard-url "$SHARD_URL_PREFIX/single-image-repeated-$NUM_IMAGES_PER_FILE-times" \
        --output-directory $DATA_DIR
    
    for arg in "--use-ray-data" "--use-tf-data"; do
        python resnet50_ray_air.py \
            --num-images-per-input-file "$NUM_IMAGES_PER_FILE" \
            --num-epochs "$NUM_EPOCHS" \
            --batch-size "$BATCH_SIZE" \
            --shuffle-buffer-size "$SHUFFLE_BUFFER_SIZE" \
            --num-images-per-epoch "$NUM_IMAGES_PER_EPOCH" \
            --train-sleep-time-ms 0 \
            --data-root "$DATA_DIR" \
            "$arg" 2>&1 | tee -a out
        sleep 5
    done
}

# Test num_images_per_file x num_images_per_epoch dimensions to check that we
# are not sensitive to file size.
NUM_IMAGES_PER_FILE=32
NUM_IMAGES_PER_EPOCH=512
run

NUM_IMAGES_PER_FILE=512
NUM_IMAGES_PER_EPOCH=512
run

NUM_IMAGES_PER_FILE=32
NUM_IMAGES_PER_EPOCH=8192
run

NUM_IMAGES_PER_FILE=512
NUM_IMAGES_PER_EPOCH=8192
run

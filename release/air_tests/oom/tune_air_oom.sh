#!/bin/bash
# Test Ray Data bulk ingestion performance as the size of input files change.

# Exit if any of the test commands fail.
set -x -e pipeline

NUM_IMAGES_PER_FILE="2048"
NUM_FILES="16"
NUM_EPOCHS=1
BATCH_SIZE=64
SHUFFLE_BUFFER_SIZE=0
DATA_DIR=/home/ray/data

SHARD_URL_PREFIX=https://air-example-data.s3.us-west-2.amazonaws.com/air-benchmarks
MAX_FILES=$( \
    echo "$NUM_FILES" \
        | sed 's/ /\n/g' \
        | python -c "import sys; print(max([int(line) for line in sys.stdin]))")
rm -rf $DATA_DIR
mkdir -p $DATA_DIR
time python air_benchmarks/mlperf-train/make_fake_dataset.py \
    --num-shards "$MAX_FILES" \
    --shard-url "$SHARD_URL_PREFIX/single-image-repeated-$NUM_IMAGES_PER_FILE-times" \
    --output-directory $DATA_DIR

for num_files in $NUM_FILES; do
    num_images_per_epoch=$((num_files * NUM_IMAGES_PER_FILE))
    time python air_benchmarks/mlperf-train/resnet50_ray_air.py \
        --num-images-per-input-file "$NUM_IMAGES_PER_FILE" \
        --num-epochs $NUM_EPOCHS \
        --batch-size $BATCH_SIZE \
        --shuffle-buffer-size $SHUFFLE_BUFFER_SIZE \
        --num-images-per-epoch $num_images_per_epoch \
        --train-sleep-time-ms 0 \
        --data-root $DATA_DIR \
        --use-ray-data \
        --trainer-resources-cpu 0 \
        --tune-trials 2
    sleep 5
done

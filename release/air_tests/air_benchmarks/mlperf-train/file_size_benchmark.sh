#!/bin/bash
# Test Ray Data bulk ingestion performance as the size of input files change.

# Exit if any of the test commands fail.
set -x -e pipeline

# NUM_IMAGES_PER_FILE=${NUM_IMAGES_PER_FILE:-"32 512 8192"}
NUM_IMAGES_PER_FILE=${NUM_IMAGES_PER_FILE:-"32"}
MIN_PARALLELISM=10
NUM_EPOCHS=1
BATCH_SIZE=64
SHUFFLE_BUFFER_SIZE=0
# DATA_DIR=/home/ray/data
DATA_DIR=~/desktop/air-benchmarks/file-size-benchmark/
DATA_DIR_PARQUET=~/desktop/air-benchmarks/parquet-32
DATA_DIR_S3=s3://anonymous@air-example-data/air-benchmarks

MAX_IMAGES_PER_FILE=$( \
    echo "$NUM_IMAGES_PER_FILE" \
        | sed 's/ /\n/g' \
        | python -c "import sys; print(max([int(line) for line in sys.stdin]))")
NUM_IMAGES_PER_EPOCH=$((MIN_PARALLELISM * MAX_IMAGES_PER_FILE))

SHARD_URL_PREFIX=https://air-example-data.s3.us-west-2.amazonaws.com/air-benchmarks

for num_images_per_file in $NUM_IMAGES_PER_FILE; do
    num_files=$(python -c "import math; print(math.ceil($NUM_IMAGES_PER_EPOCH/$num_images_per_file))")

    rm -rf $DATA_DIR
    mkdir -p $DATA_DIR
    # Generate input files locally.
    time python make_fake_dataset.py \
        --num-shards "$num_files" \
        --shard-url "$SHARD_URL_PREFIX/single-image-repeated-$num_images_per_file-times" \
        --output-directory $DATA_DIR

    # Run the benchmark, using image/binary files in local disk as the input file source.
    time python resnet50_ray_air.py \
        --num-images-per-input-file "$num_images_per_file" \
        --num-epochs $NUM_EPOCHS \
        --batch-size $BATCH_SIZE \
        --shuffle-buffer-size $SHUFFLE_BUFFER_SIZE \
        --num-images-per-epoch $NUM_IMAGES_PER_EPOCH \
        --train-sleep-time-ms 0 \
        --data-root $DATA_DIR \
        --use-ray-data \
        # --randomize-block-order
    sleep 5

    # # TODO: Run the benchmark, using parquet files in local disk as the input file source.
    # time python resnet50_ray_air.py \
    #     --num-images-per-input-file "$num_images_per_file" \
    #     --num-epochs $NUM_EPOCHS \
    #     --batch-size $BATCH_SIZE \
    #     --shuffle-buffer-size $SHUFFLE_BUFFER_SIZE \
    #     --num-images-per-epoch $NUM_IMAGES_PER_EPOCH \
    #     --train-sleep-time-ms 0 \
    #     --data-root $DATA_DIR_PARQUET \
    #     --use-ray-data \
    #     --parquet
    # sleep 5

    # Run the benchmark, using S3 as the input file source.
    time python resnet50_ray_air.py \
        --num-images-per-input-file "$num_images_per_file" \
        --num-epochs $NUM_EPOCHS \
        --batch-size $BATCH_SIZE \
        --shuffle-buffer-size $SHUFFLE_BUFFER_SIZE \
        --num-images-per-epoch $NUM_IMAGES_PER_EPOCH \
        --train-sleep-time-ms 0 \
        --data-root $DATA_DIR_S3 \
        --use-ray-data
    sleep 5
done

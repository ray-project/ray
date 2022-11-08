#!/bin/bash
# Test Ray Data bulk ingestion performance as the number of CPU nodes changes.

# Exit if any of the test commands fail.
set -x -e pipeline

NUM_FILES="128"
NUM_CPU_NODES="0 2"

NUM_EPOCHS=1
BATCH_SIZE=64
SHUFFLE_BUFFER_SIZE=0
DATA_DIR="/home/ray/data"


run_with_num_images_per_file() {
    NUM_IMAGES_PER_FILE=$1

    for num_cpu_nodes in $NUM_CPU_NODES; do
        python format_bootstrap_config.py --config-path ~/ray_bootstrap_config.yaml --num-cpu-nodes "$num_cpu_nodes"
        ray up ~/ray_bootstrap_config.yaml -y --restart-only
        
        # Wait for all nodes to start and generate data files.
        python make_fake_dataset.py \
            --num-nodes $((num_cpu_nodes + 1)) \
            --num-shards $NUM_FILES \
            --num-images-per-shard "$NUM_IMAGES_PER_FILE" \
            --output-directory $DATA_DIR

        python resnet50_ray_air.py \
            --num-images-per-input-file "$NUM_IMAGES_PER_FILE" \
            --num-epochs $NUM_EPOCHS \
            --batch-size $BATCH_SIZE \
            --shuffle-buffer-size $SHUFFLE_BUFFER_SIZE \
            --num-images-per-epoch $(( NUM_IMAGES_PER_FILE * NUM_FILES )) \
            --num-cpu-nodes "$num_cpu_nodes" \
            --train-sleep-time-ms 0 \
            --data-root $DATA_DIR \
            --use-ray-data
        sleep 5
    done
}

# Run with a file size that should fit in object store on the head node.
run_with_num_images_per_file 64
# Run with a file size that should require spilling on the head node.
run_with_num_images_per_file 512

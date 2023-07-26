#!/bin/bash

# Variables for cleaner handling
MODEL_ID="meta-llama/Llama-2-7b-hf"
BASE_DIR="/mnt/local_storage/ray_results"
CONFIG_DIR="./deepspeed_configs/zero_3_llama_2_7b.json"
DATA_DIR="./data"
TRAIN_PATH="${DATA_DIR}/train.jsonl"
TEST_PATH="${DATA_DIR}/test.jsonl"
TOKEN_PATH="${DATA_DIR}/tokens.json"

# Setup AWS
echo "Setting up AWS..."
./setup_aws.sh

# Prepare nodes
echo "Preparing nodes..."
python prepare_nodes.py --hf-model-id ${MODEL_ID}

# Check if data directory exists, if not, run create_dataset.py
if [ ! -d "${DATA_DIR}" ]; then
    echo "Data directory not found. Creating dataset..."
    python create_dataset.py
fi

# Dictionary to hold command-line arguments
declare -A args

# Check command-line arguments
for arg in "$@"; do
    case $arg in
        --as-test)
            args[--as-test]="--as-test"
            ;;
        # Here you can add more command-line arguments
        # For example:
        # --another-argument)
        #     args[--another-argument]="--another-argument"
        #     ;;
        *)
            ;;
    esac
done

# Construct additional arguments string
additional_args=""
for key in "${!args[@]}"; do
    additional_args+="${args[$key]} "
done

# Fine-tune the model
echo "Fine-tuning model..."
python finetune_hf_llm.py \
    -bs 16 \
    -nd 16 \
    --model_name ${MODEL_ID} \
    --output_dir ${BASE_DIR} \
    --ds-config ${CONFIG_DIR} \
    --train_path ${TRAIN_PATH} \
    --test_path ${TEST_PATH}  \
    --special_token_path ${TOKEN_PATH} \
    "$additional_args"

echo "Process completed."

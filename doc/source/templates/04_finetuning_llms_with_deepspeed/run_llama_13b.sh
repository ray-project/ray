#!/bin/bash

# Variables for cleaner handling
MODEL_ID="meta-llama/Llama-2-13b-hf"
BASE_DIR="/mnt/local_storage"
CONFIG_DIR="./deepspeed_configs/zero_3_llama_2_13b.json"
DATA_DIR="./data"
TRAIN_PATH="${DATA_DIR}/train.jsonl"
TEST_PATH="${DATA_DIR}/test.jsonl"
TOKEN_PATH="${DATA_DIR}/tokens.json"

# Make sure to exit when any command fails
set -e

# Setup AWS
echo "Setting up AWS..."
chmod +x ./setup_aws.sh && ./setup_aws.sh
if [ $? -ne 0 ]; then
    echo "Failed to setup AWS. Exiting..."
    exit 1
fi

# Prepare nodes
echo "Preparing nodes..."
python prepare_nodes.py --hf-model-id ${MODEL_ID}
if [ $? -ne 0 ]; then
    echo "Failed to prepare nodes. Exiting..."
    exit 1
fi

# Check if data directory exists, if not, run create_dataset.py
if [ ! -d "${DATA_DIR}" ]; then
    echo "Data directory not found. Creating dataset..."
    python create_dataset.py
    if [ $? -ne 0 ]; then
        echo "Failed to create dataset. Exiting..."
        exit 1
    fi
fi

# Check if --as-test was passed as an argument
if [[ "$@" == *"--as-test"* ]]; then
    params+=" --as-test"
fi

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
    $params

if [ $? -ne 0 ]; then
    echo "Failed to fine-tune the model. Exiting..."
    exit 1
fi

echo "Process completed."

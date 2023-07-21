#!/bin/bash

# Variables for cleaner handling
MODEL_ID="meta-llama/Llama-2-7b-chat-hf"
BASE_DIR="/mnt/cluster_storage/finetuning-llama"
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

# Check if --as-test argument is present
for arg in "$@"
do
    if [ $arg == "--as-test" ]; then
        TEST_ARG="--as-test"
        break
    else
        TEST_ARG=""
    fi
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
    ${TEST_ARG}

echo "Process completed."

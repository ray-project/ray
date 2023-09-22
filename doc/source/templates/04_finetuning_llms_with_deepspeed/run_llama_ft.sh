#!/bin/bash


# Function to check if data directory exists, if not, run create_dataset.py
check_and_create_dataset() {
    local data_dir=$1
    if [ ! -d "${data_dir}" ]; then
        echo "Data directory not found. Creating dataset..."
        if ! python create_dataset.py; then
            echo "Failed to create dataset. Exiting..."
            exit 1
        fi
    fi
}

# Function to fine-tune the model
fine_tune() {
    local bs=$1
    local nd=$2
    local model_name=$3
    local output_dir=$4
    local ds_config=$5
    local train_path=$6
    local test_path=$7
    local token_path=$8
    local params=("${@:9}")
    echo "Fine-tuning model..."
    if ! python finetune_hf_llm.py \
        -bs "${bs}" \
        -nd "${nd}" \
        --model_name "${model_name}" \
        --output_dir "${output_dir}" \
        --ds-config "${ds_config}" \
        --train_path "${train_path}" \
        --test_path "${test_path}"  \
        --special_token_path "${token_path}" \
        --num-checkpoints-to-keep 1 \
        --num-epochs 1 \
        "${params[@]}"; then
        echo "Failed to fine-tune the model. Exiting..."
        exit 1
    fi
}

# Variables for cleaner handling
BASE_DIR="/mnt/local_storage"
DATA_DIR="./data"
TRAIN_PATH="${DATA_DIR}/train.jsonl"
TEST_PATH="${DATA_DIR}/test.jsonl"
TOKEN_PATH="${DATA_DIR}/tokens.json"

# Parse arguments
SIZE=""
for arg in "$@"
do
    key=${arg%%=*}
    value=${arg#*=}
    if [[ "$key" == "--size" ]]; then
        SIZE=${value}
    elif [ "$arg" = "--as-test" ]; then
        params+=("--as-test")
    fi
done

# Batch size and node count
case $SIZE in
"7b")
    BS=16
    ND=16
    ;;
"13b")
    BS=16
    ND=16
    ;;
"70b")
    BS=8
    ND=32
    ;;
*)
    echo "Invalid size: ${SIZE}"
    exit 1
    ;;
esac

# Model related variables 
MODEL_ID="meta-llama/Llama-2-${SIZE}-hf"
CONFIG_DIR="./deepspeed_configs/zero_3_llama_2_${SIZE}.json"

check_and_create_dataset "${DATA_DIR}"
fine_tune "$BS" "$ND" "$MODEL_ID" "$BASE_DIR" "$CONFIG_DIR" "$TRAIN_PATH" "$TEST_PATH" "$TOKEN_PATH" "${params[@]}"

echo "Process completed."

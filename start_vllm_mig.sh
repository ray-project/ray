#!/bin/bash

# docker ps -a | grep vllm | awk '{print$1}' | xargs docker stop | xargs docker rm

# Get all available MIG GPU instances
MIG_INSTANCES=$(nvidia-smi -L | grep MIG | awk '{print $6}' | tr -d ')')

INDEX=0
# Loop through each MIG instance and start vLLM server
for INSTANCE in $MIG_INSTANCES; do
    echo "Starting vLLM server on MIG instance $INSTANCE"

    docker run --runtime nvidia --gpus device=$INSTANCE \
    -v /home/original_models/Qwen3-8B:/Qwen3-8B \
    -p $((9000 + ${INDEX})):9000 \
    -d \
    --ipc=host \
    vllm/vllm-openai:latest \
    --host 0.0.0.0 \
    --model /Qwen3-8B \
    --served-model-name Qwen3-8B \
    --max-model-len 8192
    let INDEX=${INDEX}+1
done

echo "All vLLM servers started on MIG instances: $MIG_INSTANCES"
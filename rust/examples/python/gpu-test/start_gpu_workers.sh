#!/bin/bash
# Start GPU actor workers — one per GPU.
# Usage: ./start_gpu_workers.sh <gcs_address> <nccl_init_method> [num_gpus]
#   e.g.: ./start_gpu_workers.sh 172.31.x.y:6379 tcp://172.31.z.w:29500 4

set -e

GCS_ADDRESS="$1"
NCCL_INIT="$2"
NUM_GPUS="${3:-4}"

if [ -z "$GCS_ADDRESS" ] || [ -z "$NCCL_INIT" ]; then
    echo "Usage: $0 <gcs_address> <nccl_init_method> [num_gpus]"
    exit 1
fi

# Kill any existing workers
pkill -f gpu_worker.py 2>/dev/null || true
sleep 1

export PYTHONUNBUFFERED=1

# Use PyTorch python if available (Deep Learning AMI), else system python3
PYTHON="${PYTHON:-$(command -v /opt/pytorch/bin/python3 2>/dev/null || echo python3)}"
echo "Using Python: $PYTHON ($($PYTHON --version 2>&1))"

for rank in $(seq 0 $((NUM_GPUS - 1))); do
    echo "Starting GPU worker rank=$rank gpu_id=$rank ..."
    CUDA_VISIBLE_DEVICES=$rank nohup $PYTHON /home/ubuntu/gpu_worker.py \
        --gcs-address "$GCS_ADDRESS" \
        --gpu-id "$rank" \
        --nccl-rank "$rank" \
        --nccl-world-size "$NUM_GPUS" \
        --nccl-init-method "$NCCL_INIT" \
        > /tmp/gpu_worker_${rank}.log 2>&1 &
done

echo "Waiting for workers to start..."
sleep 6

echo "=== Worker Connect Strings ==="
for rank in $(seq 0 $((NUM_GPUS - 1))); do
    grep "CONNECT_STRING=" /tmp/gpu_worker_${rank}.log || echo "Worker $rank: NOT READY"
done

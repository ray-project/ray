#!/bin/bash
set -ex

# GPU node setup for the Ray Rust GPU experiment.
# Expected AMI: Deep Learning OSS Nvidia Driver AMI GPU PyTorch 2.x (Ubuntu 22.04)
# CUDA, NVIDIA drivers, and PyTorch are pre-installed on this AMI.
#
# Usage: bash setup_gpu_node.sh [path_to_raylet_wheel]

# Activate the PyTorch conda/venv environment (Deep Learning AMI provides this)
source /opt/conda/etc/profile.d/conda.sh 2>/dev/null || true
conda activate pytorch 2>/dev/null || true

# Verify NVIDIA drivers and GPU count
echo "=== GPU Hardware ==="
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader
NUM_GPUS=$(nvidia-smi --query-gpu=index --format=csv,noheader | wc -l)
echo "Detected $NUM_GPUS GPUs"

if [ "$NUM_GPUS" -lt 2 ]; then
    echo "ERROR: Need at least 2 GPUs, found $NUM_GPUS"
    exit 1
fi

# Verify PyTorch + CUDA
echo ""
echo "=== PyTorch / CUDA ==="
python3 -c "
import torch
print(f'PyTorch {torch.__version__}')
print(f'CUDA {torch.version.cuda}')
print(f'cuDNN {torch.backends.cudnn.version()}')
print(f'GPUs: {torch.cuda.device_count()}')
for i in range(torch.cuda.device_count()):
    print(f'  [{i}] {torch.cuda.get_device_name(i)} ({torch.cuda.get_device_properties(i).total_mem // (1024**2)} MB)')
"

# Verify NCCL is available
python3 -c "
import torch.distributed as dist
print(f'NCCL available: {dist.is_nccl_available()}')
" || echo "WARNING: torch.distributed NCCL check failed"

# Install the Rust _raylet wheel (passed as argument)
if [ -n "$1" ]; then
    echo ""
    echo "=== Installing _raylet wheel ==="
    pip install "$1" --force-reinstall
    python3 -c "from _raylet import PyCoreWorker; print('_raylet import OK')"
else
    echo ""
    echo "No wheel path provided — skipping _raylet install."
    echo "Usage: bash setup_gpu_node.sh /path/to/ray_rust_raylet-*.whl"
fi

echo ""
echo "=== GPU node setup complete ==="
echo "GPUs: $NUM_GPUS"
echo "Ready to start GPU workers with start_gpu_workers.sh"

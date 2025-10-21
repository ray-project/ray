# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/deplocks/llm/rayllm_*.lock ./

ARG KVER="5.15.0-139-generic"

RUN <<EOF
#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"

# ray-llm image only support cuda 12.8
CUDA_CODE=cu128

if [[ "${PYTHON_CODE}" != "py311" ]]; then
    echo "ray-llm only support Python 3.11 now (this image is for ${PYTHON_CODE})."
    exit 1
fi

uv pip install --system --no-cache-dir --no-deps \
    --index-strategy unsafe-best-match \
    -r "rayllm_${PYTHON_CODE}_${CUDA_CODE}.lock"

# Export installed packages
$HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

sudo apt-get update -y && sudo apt-get install -y kmod pkg-config librdmacm-dev cmake

# Install DeepEP + PPLX kernels via vLLM installation script
mkdir work
cd work
export CUDA_HOME=/usr/local/cuda
git clone https://github.com/vllm-project/vllm.git
cd vllm
git checkout v0.11.0
cd tools/ep_kernels/
TORCH_CUDA_ARCH_LIST="9.0" bash install_python_libraries.sh

sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean

EOF

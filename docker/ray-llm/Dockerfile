# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/deplocks/llm/rayllm_*.lock ./

# vLLM version tag to use for EP kernel and DeepGEMM install scripts
# Keep in sync with vllm version in python/requirements/llm/llm-requirements.txt
ARG VLLM_SCRIPTS_REF="v0.15.0"

RUN <<EOF
#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"

if [[ "${PYTHON_CODE}" == "py311" ]]; then
    CUDA_CODE=cu128
    # Use nvshmem 3.3.20 which was compiled with CUDA 12.8 to be compatible with Ray LLM image built with CUDA 12.8
    NVSHMEM_VER=3.3.20
elif [[ "${PYTHON_CODE}" == "py312" ]]; then
    CUDA_CODE=cu129
    # Use nvshmem 3.3.24 which is the default for vLLM and compatible with CUDA 12.9 and 13
    # https://github.com/vllm-project/vllm/blob/64ac1395e8d52e3e38910a62c7eb8524126730d8/tools/ep_kernels/install_python_libraries.sh#L14
    NVSHMEM_VER=3.3.24
else
    echo "ray-llm supports Python 3.11 and 3.12 only (this image is ${PYTHON_CODE})."
    exit 1
fi

uv pip install --system --no-cache-dir --no-deps \
    --index-strategy unsafe-best-match \
    -r "rayllm_${PYTHON_CODE}_${CUDA_CODE}.lock"

# Export installed packages
$HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

sudo apt-get update -y && sudo apt-get install -y curl kmod pkg-config librdmacm-dev cmake

# Fetch and run vLLM install scripts at pinned commit
VLLM_RAW="https://raw.githubusercontent.com/vllm-project/vllm/${VLLM_SCRIPTS_REF}"

# Tell uv to use system Python since the vLLM scripts use uv
export UV_SYSTEM_PYTHON=1

# Set CUDA architectures for building EP kernels
# EP kernels + DeepGEMM require Hopper+ features (matches vLLM Dockerfile)
export TORCH_CUDA_ARCH_LIST="9.0a 10.0a"

# Install EP kernels (PPLX, DeepEP, and NVSHMEM)
curl -fsSL "${VLLM_RAW}/tools/ep_kernels/install_python_libraries.sh" | \
    bash -s -- --workspace /home/ray/llm_ep_support --nvshmem-ver ${NVSHMEM_VER}

# Install DeepGEMM
curl -fsSL "${VLLM_RAW}/tools/install_deepgemm.sh" | bash

sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean

EOF

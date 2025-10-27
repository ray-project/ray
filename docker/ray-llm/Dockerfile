# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/deplocks/llm/rayllm_*.lock ./

ARG KVER="5.15.0-139-generic"

ENV NVSHMEM_HOME=/usr/local/nvshmem
ENV LD_LIBRARY_PATH=/usr/local/nvshmem/lib:${LD_LIBRARY_PATH}
ENV PATH=/usr/local/nvshmem/bin:${PATH}
ENV RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY=1
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so:${LD_PRELOAD}
ENV CUDA_HOME=/usr/local/cuda

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

# The following roughly matches vllm/tools/ep_kernels/install_python_libraries.sh
# with the addition of DeepGEMM and updated NVSHMEM version

# Install DeepEP kernels
ROOT_DIR="/home/ray"
EP_DIR="${ROOT_DIR}/ep"
mkdir -p "${EP_DIR}"

# disable all features except IBGDA
export NVSHMEM_IBGDA_SUPPORT=1
export NVSHMEM_SHMEM_SUPPORT=0
export NVSHMEM_UCX_SUPPORT=0
export NVSHMEM_USE_NCCL=0
export NVSHMEM_PMIX_SUPPORT=0
export NVSHMEM_TIMEOUT_DEVICE_POLLING=0
export NVSHMEM_USE_GDRCOPY=0
export NVSHMEM_IBRC_SUPPORT=0
export NVSHMEM_BUILD_TESTS=0
export NVSHMEM_BUILD_EXAMPLES=0
export NVSHMEM_MPI_SUPPORT=0
export NVSHMEM_BUILD_HYDRA_LAUNCHER=0
export NVSHMEM_BUILD_TXZ_PACKAGE=0

NVSHMEM_VERSION="3.4.5"
(
    echo "Installing NVSHMEM ${NVSHMEM_VERSION}"

    cd "${EP_DIR}"
    mkdir -p nvshmem_src
    wget https://developer.download.nvidia.com/compute/redist/nvshmem/${NVSHMEM_VERSION}/source/nvshmem_src_cuda12-all-all-${NVSHMEM_VERSION}.tar.gz
    tar -xvf nvshmem_src_cuda12-all-all-${NVSHMEM_VERSION}.tar.gz -C nvshmem_src --strip-components=1
    cd nvshmem_src

    cmake -G Ninja -S . -B "${EP_DIR}/nvshmem_build" -DCMAKE_INSTALL_PREFIX="${EP_DIR}/nvshmem_install"
    cmake --build "${EP_DIR}/nvshmem_build" --target install
)

# Install PPLX Kernels
(
    echo "Installing PPLX Kernels"

    cd "${EP_DIR}"

    export CMAKE_PREFIX_PATH="${EP_DIR}/nvshmem_install"

    # build and install pplx, require pytorch installed
    git clone --depth 1 --no-checkout https://github.com/ppl-ai/pplx-kernels
    cd pplx-kernels
    # using a specific commit to make the build deterministic:
    # https://github.com/ppl-ai/pplx-kernels/commit/1d76f488d794f01dc0e895cd746b235392379757
    git fetch --depth 1 origin 1d76f488d794f01dc0e895cd746b235392379757
    git checkout 1d76f488d794f01dc0e895cd746b235392379757
    # see https://github.com/pypa/pip/issues/9955#issuecomment-838065925
    # PIP_NO_BUILD_ISOLATION=0 disables build isolation
    PIP_NO_BUILD_ISOLATION=0 TORCH_CUDA_ARCH_LIST=9.0a+PTX pip install . --no-deps -v
)

# Install DeepEP Kernels
{
    echo "Installing DeepEP Kernels"

    cd "${EP_DIR}"

    git clone https://github.com/deepseek-ai/DeepEP.git
    cd DeepEP
    git checkout 73b6ea4
    export NVSHMEM_DIR="${EP_DIR}/nvshmem_install"
    TORCH_CUDA_ARCH_LIST=9.0 pip install --no-build-isolation -vvv -e .
}

# Install DeepGEMM
(
    echo "Installing DeepGEMM"

    cd "${EP_DIR}"

    git clone --recursive https://github.com/deepseek-ai/DeepGEMM.git
    cd DeepGEMM

    cat install.sh
    ./install.sh
)

sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean

EOF

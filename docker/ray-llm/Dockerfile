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

# Install DeepEP kernels
EP_TEMP_DIR=$(pwd)/"ep_temp_dir"
mkdir -p "${EP_TEMP_DIR}"

NVSHMEM_VERSION="3.2.5-1"
(
    echo "Installing NVSHMEM ${NVSHMEM_VERSION}"

    cd "${EP_TEMP_DIR}"
    mkdir -p nvshmem_src
    wget https://developer.download.nvidia.com/compute/redist/nvshmem/3.2.5/source/nvshmem_src_${NVSHMEM_VERSION}.txz
    tar -xvf nvshmem_src_${NVSHMEM_VERSION}.txz -C nvshmem_src --strip-components=1
    cd nvshmem_src
    # using a specific commit to make the build deterministic:
    # https://github.com/deepseek-ai/DeepEP/commit/bdd119f8b249953cab366f4d737ad39d4246fd7e
    wget https://github.com/deepseek-ai/DeepEP/raw/bdd119f8b249953cab366f4d737ad39d4246fd7e/third-party/nvshmem.patch
    git init
    git apply -vvv nvshmem.patch
    wget https://github.com/vllm-project/vllm/raw/releases/v0.10.0/tools/ep_kernels/elastic_ep/eep_nvshmem.patch
    git apply --reject --whitespace=fix eep_nvshmem.patch

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

    cmake -G Ninja -S . -B "${EP_TEMP_DIR}/nvshmem_build" -DCMAKE_INSTALL_PREFIX="${EP_TEMP_DIR}/nvshmem_install"
    cmake --build "${EP_TEMP_DIR}/nvshmem_build" --target install
)

# Install PPLX Kernels
(
    echo "Installing PPLX Kernels"

    cd "${EP_TEMP_DIR}"

    export CMAKE_PREFIX_PATH="${EP_TEMP_DIR}/nvshmem_install"

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

rm -rf "${EP_TEMP_DIR}"

sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean

EOF

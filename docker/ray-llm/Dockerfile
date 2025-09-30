# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/deplocks/llm/rayllm_*.lock ./

ARG KVER="5.15.0-139-generic"
ARG ROOT_DIR="/usr/local"
ARG GDR_HOME="${ROOT_DIR}/gdrcopy"
ARG UCX_HOME="${ROOT_DIR}/ucx"
ARG NIXL_HOME="${ROOT_DIR}/nixl"

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

# Begin NIXL installation

mkdir -p "${ROOT_DIR}"

CUDACXX=$(which nvcc)
CUDA_HOME=$(dirname $(dirname ${CUDACXX}))

TEMP_DIR="nixl_installer"
mkdir -p "${TEMP_DIR}"

sudo apt-get update
# kmod needed by nvidia-installer, pkg-config needed by GDRCopy, librdmacm-dev needed by UCX
sudo apt-get install -y kmod pkg-config librdmacm-dev cmake

(
    echo "Installing GDRCopy"
    cd "${TEMP_DIR}"
    [[ -d "/lib/modules/${KVER}" ]] || sudo apt-get install linux-headers-${KVER} -y
    NV_DRIVER_VERSION="570.153.02"
    wget "https://us.download.nvidia.com/XFree86/Linux-x86_64/${NV_DRIVER_VERSION}/NVIDIA-Linux-x86_64-${NV_DRIVER_VERSION}.run" -q
    sh NVIDIA-Linux-x86_64-${NV_DRIVER_VERSION}.run -x
    sudo NVIDIA-Linux-x86_64-${NV_DRIVER_VERSION}/nvidia-installer \
        --silent \
        --no-questions \
        --no-install-compat32-libs \
        --kernel-source-path="/lib/modules/${KVER}/build" \
        --utility-prefix="/usr"

    (
        wget "https://github.com/NVIDIA/gdrcopy/archive/refs/tags/v2.5.tar.gz" -q
        tar xzf v2.5.tar.gz; rm v2.5.tar.gz
        cd gdrcopy-2.5
        sudo make prefix=$GDR_HOME CUDA=$CUDA_HOME KVER=${KVER} all install
    )

    # Uninstall the driver, this driver might have conflict with the library
    # version on host. Remove it from container.
    sudo NVIDIA-Linux-x86_64-${NV_DRIVER_VERSION}/nvidia-installer \
        --uninstall \
        --silent \
        --no-questions
)

UCX_VERSION="1.19.0"
(
    echo "Installing UCX ${UCX_VERSION}"
    cd "${TEMP_DIR}"
    wget "https://github.com/openucx/ucx/releases/download/v${UCX_VERSION}/ucx-${UCX_VERSION}.tar.gz" -q
    tar xzf "ucx-${UCX_VERSION}.tar.gz"; rm "ucx-${UCX_VERSION}.tar.gz"
    cd "ucx-${UCX_VERSION}"

    # Additional options for Mellanox NICs, install by default
    MLX_OPTS="--with-rdmacm \
              --with-mlx5-dv \
              --with-ib-hw-tm"

    ./configure --prefix=${UCX_HOME}               \
                --enable-shared                    \
                --disable-static                   \
                --disable-doxygen-doc              \
                --enable-optimizations             \
                --enable-cma                       \
                --enable-devel-headers             \
                --with-cuda=${CUDA_HOME}           \
                --with-dm                          \
                --with-gdrcopy=${GDR_HOME}         \
                --with-verbs                       \
                --enable-mt                        \
                ${MLX_OPTS}
    make -j
    sudo make -j install-strip

    sudo ldconfig
)

# Keep in sync with llm-requirements.txt
NIXL_VERSION="0.4.1"
(
    echo "Installing NIXL ${NIXL_VERSION}"
    # NIXL needs meson pybind11 ninja, but should have been included in requirements_*.txt
    cd "${TEMP_DIR}"
    wget "https://github.com/ai-dynamo/nixl/archive/refs/tags/${NIXL_VERSION}.tar.gz" -q
    tar xzf "${NIXL_VERSION}.tar.gz"; rm "${NIXL_VERSION}.tar.gz"
    cd "nixl-${NIXL_VERSION}"
    meson setup build --prefix=${NIXL_HOME} -Ducx_path=${UCX_HOME}
    cd build
    ninja
    sudo env "PATH=$PATH" ninja install
)
sudo rm -rf "${TEMP_DIR}"

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

ENV PATH="${PATH}:${UCX_HOME}/bin:${NIXL_HOME}/bin"
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${UCX_HOME}/lib:${NIXL_HOME}/lib/x86_64-linux-gnu"
ENV NIXL_PLUGIN_DIR="${NIXL_HOME}/lib/x86_64-linux-gnu/plugins/"

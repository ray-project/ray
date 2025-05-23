# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/requirements_*.txt ./

ARG ROOT_DIR="/usr/local"
ARG GDR_HOME="${ROOT_DIR}/gdrcopy"
ARG UCX_HOME="${ROOT_DIR}/ucx"
ARG NIXL_HOME="${ROOT_DIR}/nixl"

RUN <<EOF
#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"

# ray-llm image only support cuda 12.4 for now.
CUDA_CODE=cu124

if [[ "${PYTHON_CODE}" != "py311" ]]; then
    echo "ray-llm only support Python 3.11 now (this image is for ${PYTHON_CODE})."
    exit 1
fi

pip install --no-deps -r "requirements_compiled_rayllm_${PYTHON_CODE}_${CUDA_CODE}.txt"

# Export installed packages
$HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Install NIXL
ARCH=x86_64

mkdir -p "${ROOT_DIR}"

sudo apt-get update
sudo apt-get install -y pkg-config

CUDA_HOME=`dirname $(dirname $(which nvcc))`

TEMP_DIR="nixl_installer"
mkdir -p "${TEMP_DIR}"

(
    echo "Installing GDRCopy"
    cd "${TEMP_DIR}"
    wget https://github.com/NVIDIA/gdrcopy/archive/refs/tags/v2.5.tar.gz
    tar xzf v2.5.tar.gz; rm v2.5.tar.gz
    cd gdrcopy-2.5
    make prefix=$GDR_HOME CUDA=$CUDA_HOME all install
    sudo ./insmod.sh
)

(
    echo "Installing UCX"
    cd "${TEMP_DIR}"
    wget https://github.com/openucx/ucx/releases/download/v1.18.0/ucx-1.18.0.tar.gz
    tar xzf ucx-1.18.0.tar.gz; rm ucx-1.18.0.tar.gz
    cd ucx-1.18.0

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
    make -j install-strip

    sudo ldconfig
)
rm -rf "${TEMP_DIR}/ucx-1.18.0"

(
    echo "Installing NIXL"
    cd "${TEMP_DIR}"
    wget https://github.com/ai-dynamo/nixl/archive/refs/tags/0.2.0.tar.gz
    tar xzf 0.2.0.tar.gz; rm 0.2.0.tar.gz
    cd nixl-0.2.0
    meson setup build --prefix=${NIXL_HOME} -Ducx_path=${UCX_HOME}
    cd build
    ninja
    ninja install
)
rm -rf "${TEMP_DIR}/nixl-0.2.0"

EOF

ENV PATH="${UCX_HOME}/bin:${NIXL_HOME}/bin:${PATH}"
ENV LD_LIBRARY_PATH="${UCX_HOME}/lib:${NIXL_HOME}/lib/${ARCH}-linux-gnu:${LD_LIBRARY_PATH}"

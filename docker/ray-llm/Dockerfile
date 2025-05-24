# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/requirements_*.txt ./

ARG KVER="5.15.0-139-generic"
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

# Begin NIXL installation

mkdir -p "${ROOT_DIR}"

CUDA_HOME=`dirname $(dirname $(which nvcc))`

TEMP_DIR="nixl_installer"
mkdir -p "${TEMP_DIR}"

(
    echo "Installing GDRCopy"
    cd "${TEMP_DIR}"
    sudo apt-get update
    # Needed by GDRCopy
    sudo apt-get install -y pkg-config
    # Needed by nvidia-installer
    sudo apt-get install -y kmod
    ls /lib/modules/${KVER} || sudo apt install linux-headers-${KVER} -y
    NV_DRIVER_VERSION="570.153.02"
    wget https://us.download.nvidia.com/XFree86/Linux-x86_64/${NV_DRIVER_VERSION}/NVIDIA-Linux-x86_64-${NV_DRIVER_VERSION}.run
    sh NVIDIA-Linux-x86_64-${NV_DRIVER_VERSION}.run -x
    sudo NVIDIA-Linux-x86_64-${NV_DRIVER_VERSION}/nvidia-installer \
        --silent \
        --no-questions \
        --no-install-compat32-libs \
        --kernel-source-path="/lib/modules/${KVER}/build" \
        --utility-prefix="/usr"

    wget https://github.com/NVIDIA/gdrcopy/archive/refs/tags/v2.5.tar.gz
    tar xzf v2.5.tar.gz; rm v2.5.tar.gz
    cd gdrcopy-2.5
    sudo make prefix=$GDR_HOME CUDA=$CUDA_HOME KVER=${KVER} all install
    # sudo ./insmod.sh
)

(
    echo "Installing UCX"
    # Needed by UCX
    sudo apt-get install -y librdmacm-dev
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
    sudo make -j install-strip

    sudo ldconfig
)
rm -rf "${TEMP_DIR}/ucx-1.18.0"

(
    echo "Installing NIXL"
    # Needed by NIXL
    pip install --no-deps meson pybind11 ninja
    cd "${TEMP_DIR}"
    wget https://github.com/ai-dynamo/nixl/archive/refs/tags/0.2.0.tar.gz
    tar xzf 0.2.0.tar.gz; rm 0.2.0.tar.gz
    cd nixl-0.2.0
    meson setup build --prefix=${NIXL_HOME} -Ducx_path=${UCX_HOME}
    cd build
    ninja
    NINJA_PATH=$(which ninja)
    sudo ${NINJA_PATH} install
)
sudo rm -rf "${TEMP_DIR}"

EOF

ENV PATH="${UCX_HOME}/bin:${NIXL_HOME}/bin:${PATH}"
ENV LD_LIBRARY_PATH="${UCX_HOME}/lib:${NIXL_HOME}/lib/${HOSTTYPE}-linux-gnu:${LD_LIBRARY_PATH}"

# syntax=docker/dockerfile:1.3-labs

# The base-deps Docker image installs main libraries needed to run Ray

# The GPU options are NVIDIA CUDA developer images.
ARG BASE_IMAGE="ubuntu:22.04"

FROM ${BASE_IMAGE}
# If this arg is not "autoscaler" then no autoscaler requirements will be included
ENV TZ=America/Los_Angeles
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# TODO(ilr) $HOME seems to point to result in "" instead of "/home/ray"
# Q: Why add paths like /usr/local/nvidia/lib64 and /usr/local/nvidia/bin?
# A: The NVIDIA GPU operator version used by GKE injects these into the container
# after it's mounted to a pod.
# Issue is tracked here:
# https://github.com/GoogleCloudPlatform/compute-gpu-installation/issues/46
# More context here:
# https://github.com/NVIDIA/nvidia-container-toolkit/issues/275
# and here:
# https://gitlab.com/nvidia/container-images/cuda/-/issues/27
ENV PATH "/home/ray/anaconda3/bin:$PATH:/usr/local/nvidia/bin"
ENV LD_LIBRARY_PATH "$LD_LIBRARY_PATH:/usr/local/nvidia/lib64"
ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.10
ARG CONSTRAINTS_FILE="python/requirements_compiled_py${PYTHON_VERSION}.txt"
ARG PYTHON_DEPSET="python/deplocks/base_deps/ray_base_deps_py${PYTHON_VERSION}.lock"

ARG RAY_UID=1000
ARG RAY_GID=100

# HAProxy package and source build parameters
ARG HAPROXY_REPO="https://www.haproxy.com/download/haproxy/performance/ubuntu/ha32"
ARG HAPROXY_KEY_URL="https://pks.haproxy.com/linux/community/RPM-GPG-KEY-HAProxy"
ARG AWSLC_TAG="v1.69.0"
ARG HAPROXY_BRANCH="3.2"
ARG HAPROXY_MINOR="3.2.15"
ARG HAPROXY_SHA256="117e408aff544c9ad758c2fb3fd8cf791a72609d3ae319b2cf9f2a0b035393c2"
ARG HAPROXY_SRC_URL="https://www.haproxy.org/download"

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -y
apt-get upgrade -y

#ca-certificates should prevent errors from wget
APT_PKGS=(
    sudo
    tzdata
    ca-certificates
    git
    libjemalloc-dev
    wget
    cmake
    g++
    zlib1g-dev

    # For autoscaler
    tmux
    screen
    rsync
    netbase
    openssh-client
    gnupg

    # For HAProxy
    socat
)

apt-get install -y --no-install-recommends "${APT_PKGS[@]}"

# Install HAProxy 3.2 based on architecture
DPKG_ARCH=$(dpkg --print-architecture)
if [ "$DPKG_ARCH" = "amd64" ]; then
    # Use HAProxy performance package (haproxy-awslc) from HAProxy repo
    install -d -m 0755 /usr/share/keyrings && \
    wget -qO /usr/share/keyrings/HAPROXY-key-community.asc "${HAPROXY_KEY_URL}" && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/HAPROXY-key-community.asc] ${HAPROXY_REPO} jammy main" \
        > /etc/apt/sources.list.d/haproxy.list && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends haproxy-awslc && \
    ln -s /usr/sbin/haproxy /usr/local/bin/haproxy
else
    # Build HAProxy from source with AWS-LC on non-amd64 platforms
    apt-get install -y --no-install-recommends \
        ca-certificates curl golang ninja-build \
        make libc6-dev libssl-dev libpcre2-dev liblua5.4-dev && \
    git clone --depth 1 --branch "${AWSLC_TAG}" https://github.com/aws/aws-lc.git /tmp/aws-lc && \
    mkdir /tmp/aws-lc/build && \
    cd /tmp/aws-lc/build && \
    cmake -G Ninja -DCMAKE_INSTALL_PREFIX=/opt/aws-lc -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON .. && \
    ninja install && \
    rm -rf /tmp/aws-lc && \
    curl -sfSL "${HAPROXY_SRC_URL}/${HAPROXY_BRANCH}/src/haproxy-${HAPROXY_MINOR}.tar.gz" -o /tmp/haproxy.tar.gz && \
    echo "${HAPROXY_SHA256} */tmp/haproxy.tar.gz" | sha256sum -c - && \
    mkdir -p /tmp/haproxy && \
    tar -xzf /tmp/haproxy.tar.gz -C /tmp/haproxy --strip-components=1 && \
    rm -f /tmp/haproxy.tar.gz && \
    make -C /tmp/haproxy -j"$(nproc)" TARGET=linux-glibc CPU=generic \
        USE_PCRE2=1 USE_PCRE2_JIT=1 \
        USE_TFO=1 USE_LINUX_TPROXY=1 USE_LUA=1 USE_GETADDRINFO=1 \
        USE_PROMEX=1 USE_SLZ=1 \
        USE_OPENSSL_AWSLC=1 USE_PTHREAD_EMULATION=1 \
        SSL_INC=/opt/aws-lc/include SSL_LIB=/opt/aws-lc/lib USE_QUIC=1 \
        LDFLAGS="-L/opt/aws-lc/lib -Wl,-rpath,/opt/aws-lc/lib" \
        ADDLIB=-ljemalloc \
        all && \
    make -C /tmp/haproxy TARGET=linux-glibc install-bin && \
    ln -s /usr/local/sbin/haproxy /usr/local/bin/haproxy && \
    rm -rf /tmp/haproxy && \
    echo "/opt/aws-lc/lib" > /etc/ld.so.conf.d/awslc.conf && \
    mkdir -p /opt/aws-lc/ssl && \
    rm -rf /opt/aws-lc/ssl/certs && \
    ln -s /etc/ssl/certs /opt/aws-lc/ssl/certs && \
    ldconfig && \
    apt-get purge -y golang ninja-build libssl-dev libpcre2-dev liblua5.4-dev libc6-dev && \
    apt-get autoremove -y && \
    apt-get install -y --no-install-recommends liblua5.4-0 libpcre2-8-0
fi

useradd -ms /bin/bash -d /home/ray ray --uid $RAY_UID --gid $RAY_GID
usermod -aG sudo ray
echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers

mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy
chown -R ray:"$(id -gn ray)" /run/haproxy

EOF

USER $RAY_UID
ENV HOME=/home/ray
WORKDIR /home/ray

COPY --chown=ray "$CONSTRAINTS_FILE" /home/ray/requirements_compiled.txt
COPY --chown=ray "$PYTHON_DEPSET" /home/ray/python_depset.lock

SHELL ["/bin/bash", "-c"]

RUN <<EOF
#!/bin/bash

set -euo pipefail

# Determine the architecture of the host
if [[ "${HOSTTYPE}" =~ ^x86_64 ]]; then
    ARCH="x86_64"
elif [[ "${HOSTTYPE}" =~ ^aarch64 ]]; then
    ARCH="aarch64"
else
    echo "Unsupported architecture ${HOSTTYPE}" >/dev/stderr
    exit 1
fi

# Install miniforge
wget --quiet \
    "https://github.com/conda-forge/miniforge/releases/download/24.11.3-0/Miniforge3-24.11.3-0-Linux-${ARCH}.sh" \
    -O /tmp/miniforge.sh

/bin/bash /tmp/miniforge.sh -b -u -p $HOME/anaconda3

$HOME/anaconda3/bin/conda init
echo 'export PATH=$HOME/anaconda3/bin:$PATH' >> $HOME/.bashrc
rm /tmp/miniforge.sh
$HOME/anaconda3/bin/conda install -y libgcc-ng python=$PYTHON_VERSION
$HOME/anaconda3/bin/conda install -y -c conda-forge libffi=3.4.2
$HOME/anaconda3/bin/conda clean -y --all

# Install uv
wget -qO- https://astral.sh/uv/install.sh | sudo -E env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

# Set up Conda as system Python
export PATH=$HOME/anaconda3/bin:$PATH

# Some packages are on PyPI as well as other indices, but the latter
# (unhelpfully) take precedence. We use `--index-strategy unsafe-best-match`
# to ensure that the best match is chosen from the available indices.
uv pip install --system --no-cache-dir --no-deps --index-strategy unsafe-best-match \
    -r $HOME/python_depset.lock

# We install cmake temporarily to get psutil
sudo apt-get autoremove -y cmake zlib1g-dev

# We keep g++ on GPU images, because uninstalling removes CUDA Devel tooling
if [[ ! -d /usr/local/cuda ]]; then
    sudo apt-get autoremove -y g++
fi

sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean

EOF

WORKDIR $HOME

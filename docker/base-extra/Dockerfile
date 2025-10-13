# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE="rayproject/ray:latest"

FROM "$BASE_IMAGE" AS main-build

ENV TERM=xterm

ARG SSH_PORT=5020

RUN <<EOF
#!/bin/bash

set -exuo pipefail

if [[ "$HOSTTYPE" =~ ^x86_64 ]]; then
    ARCH="x86_64"
elif [[ "$HOSTTYPE" =~ ^aarch64 ]]; then
    ARCH="aarch64"
else
    echo "Unsupported architecture $MACHTYPE" >/dev/stderr
    exit 1
fi

# Create boto config; makes gsutil happy.
echo "[GoogleCompute]" > "${HOME}/.boto"
echo "service_account = default" >> "${HOME}/.boto"
chmod 600 "${HOME}/.boto"

if [[ "$ARCH" == "x86_64" ]]; then
    sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/3bf863cc.pub
    sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/machine-learning/repos/ubuntu1804/x86_64/7fa2af80.pub
else
    sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/arm64/7fa2af80.pub
    # Nvidia does not have machine-learning repo for arm64
fi

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
wget -O - https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Add gdb since ray dashboard uses `memray attach`, which requires gdb.

APT_PKGS=(
    google-cloud-sdk
    supervisor
    vim
    zsh
    nfs-common
    zip
    unzip
    build-essential
    ssh
    curl
    gdb
)

sudo apt-get update -y
sudo apt-get install -y "${APT_PKGS[@]}"
sudo apt-get autoclean

# Install azcopy
AZCOPY_VERSION="10.30.0"
AZCOPY_TMP="$(mktemp -d)"
(
    cd "${AZCOPY_TMP}"
    if [[ "$ARCH" == "x86_64" ]]; then
        curl -sSfL "https://github.com/Azure/azure-storage-azcopy/releases/download/v${AZCOPY_VERSION}/azcopy_linux_amd64_${AZCOPY_VERSION}.tar.gz" \
            -o- | tar -xz "azcopy_linux_amd64_${AZCOPY_VERSION}/azcopy"
        sudo mv "azcopy_linux_amd64_${AZCOPY_VERSION}/azcopy" /usr/local/bin/azcopy
    else
        curl -sSfL "https://github.com/Azure/azure-storage-azcopy/releases/download/v${AZCOPY_VERSION}/azcopy_linux_arm64_${AZCOPY_VERSION}.tar.gz" \
            -o- | tar -xz "azcopy_linux_arm64_${AZCOPY_VERSION}/azcopy"
        sudo mv "azcopy_linux_arm64_${AZCOPY_VERSION}/azcopy" /usr/local/bin/azcopy
    fi
)
rm -rf "${AZCOPY_TMP}"

# Install dynolog, only on x86_64 machines.
if [[ "$ARCH" == "x86_64" ]]; then
    DYNOLOG_TMP="$(mktemp -d)"
    (
        cd "${DYNOLOG_TMP}"
        curl -sSL https://github.com/facebookincubator/dynolog/releases/download/v0.3.2/dynolog_0.3.2-0-amd64.deb -o dynolog_0.3.2-0-amd64.deb
        sudo dpkg -i dynolog_0.3.2-0-amd64.deb
    )
    rm -rf "${DYNOLOG_TMP}"
fi

# Python dependencies to install. To specify a version, please make the change
# in OSS ray repository, but not here.
PYTHON_REQUIREMENTS=(
    azure-identity
    jupyterlab
    ipywidgets
    grpcio
    grpcio-tools

    # Pinning jupyter_server_terminals==0.4.4 , the higher version will break the
    # webterminal when using an older version of terminado.
    jupyter_server_terminals==0.4.4

    # [backend] is for installing anyscale CLI for use in the anyscale cloud.
    "anyscale[backend]"
)


PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

uv pip install --system --no-cache-dir --index-strategy unsafe-best-match \
    -c /home/ray/requirements_compiled.txt \
    "${PYTHON_REQUIREMENTS[@]}"

# Install awscli v2
AWSCLI_TMP="$(mktemp -d)"
(
    cd "${AWSCLI_TMP}"
    curl -sfL "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install
)
rm -rf "${AWSCLI_TMP}"

# Cleanup unused packages and caches.
$HOME/anaconda3/bin/conda clean -y -all

# Work around for https://bugs.launchpad.net/ubuntu/+source/openssh/+bug/45234
sudo mkdir -p /var/run/sshd
# Configure ssh port
echo Port $SSH_PORT | sudo tee -a /etc/ssh/sshd_config

if [[ ! -d /usr/local/cuda ]]; then
    EFA_VERSION="1.42.0"
    GDRCOPY_VERSION=""
    AWS_OFI_NCCL_VERSION=""
elif [[ -d "/usr/local/cuda-11" ]]; then
    EFA_VERSION="1.28.0"
    GDRCOPY_VERSION="2.4"
    AWS_OFI_NCCL_VERSION="1.7.3-aws"
elif [[ -d "/usr/local/cuda-12" ]]; then
    EFA_VERSION="1.42.0"
    GDRCOPY_VERSION="2.5"
    AWS_OFI_NCCL_VERSION="1.15.0"
else
    echo "Unsupported CUDA major version"
    exit 1
fi

# Install EFA
wget -q "https://efa-installer.amazonaws.com/aws-efa-installer-${EFA_VERSION}.tar.gz" -O "/tmp/aws-efa-installer-${EFA_VERSION}.tar.gz"
wget -q "https://efa-installer.amazonaws.com/aws-efa-installer.key" -O /tmp/aws-efa-installer.key && gpg --import /tmp/aws-efa-installer.key
gpg --fingerprint </tmp/aws-efa-installer.key
wget -q "https://efa-installer.amazonaws.com/aws-efa-installer-${EFA_VERSION}.tar.gz.sig" -O "/tmp/aws-efa-installer-${EFA_VERSION}.tar.gz.sig"
gpg --verify "/tmp/aws-efa-installer-${EFA_VERSION}.tar.gz.sig"
tar -xzf "/tmp/aws-efa-installer-${EFA_VERSION}.tar.gz" -C /tmp
(cd /tmp/aws-efa-installer; sudo bash efa_installer.sh --yes --skip-kmod)
rm -rf "/tmp/aws-efa-installer-${EFA_VERSION}.tar.gz" /tmp/aws-efa-installer.key /tmp/aws-efa-installer

# Install GDRCopy
if [[ "${GDRCOPY_VERSION}" != "" ]]; then
    echo "Installing gdrcopy for GPU images"
    sudo apt-get -y install build-essential devscripts debhelper check libsubunit-dev fakeroot pkg-config dkms
    wget -q "https://github.com/NVIDIA/gdrcopy/archive/refs/tags/v${GDRCOPY_VERSION}.tar.gz" -O "/tmp/v${GDRCOPY_VERSION}.tar.gz"
    tar -xzf "/tmp/v${GDRCOPY_VERSION}.tar.gz" -C /tmp
    (
        cd "/tmp/gdrcopy-${GDRCOPY_VERSION}"
        sudo make -j`nproc` lib_install
    )
    rm -rf "/tmp/gdrcopy-${GDRCOPY_VERSION}"
else
    echo "Skip installing gdrcopy"
fi

# Install AWS OFI NCCL
if [[ "${AWS_OFI_NCCL_VERSION}" != "" ]]; then
    echo "Installing aws-ofi-nccl"
    sudo apt-get install -y autoconf libhwloc-dev
    (
        cd /tmp
        git clone --depth=1 "https://github.com/aws/aws-ofi-nccl.git" -b "v${AWS_OFI_NCCL_VERSION}"
    )
    (
        cd /tmp/aws-ofi-nccl
        ./autogen.sh
        ./configure --with-libfabric=/opt/amazon/efa \
            --with-mpi=/opt/amazon/openmpi \
            --with-cuda=/usr/local/cuda \
            --with-nccl=/usr/local --prefix=/usr/local
        make -j`nproc`
        sudo make install
    )
    rm -rf /tmp/aws-ofi-nccl
else
    echo "Skip installing aws-ofi-nccl"
fi

# Remove apt sources so that it won't run into apt update issues when running
# the image.
sudo rm -rf /etc/apt/sources.list.d/*
sudo rm -rf /var/lib/apt/lists/*

EOF

# --- HAProxy Build Stage ---
FROM $BASE_IMAGE AS haproxy-builder

USER root

RUN <<EOF
#!/bin/bash
set -euo pipefail

apt-get update -y
apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    libc6-dev \
    liblua5.3-dev \
    libpcre3-dev \
    libssl-dev \
    zlib1g-dev

rm -rf /var/lib/apt/lists/*

# Install HAProxy from source
HAPROXY_VERSION="2.8.12"
HAPROXY_BUILD_DIR=$(mktemp -d)
curl -sSfL -o "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" "https://www.haproxy.org/download/2.8/src/haproxy-${HAPROXY_VERSION}.tar.gz"
tar -xzf "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" -C "${HAPROXY_BUILD_DIR}" --strip-components=1
make -C "${HAPROXY_BUILD_DIR}" TARGET=linux-glibc USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc)
make -C "${HAPROXY_BUILD_DIR}" install SBINDIR=/usr/local/bin
rm -rf "${HAPROXY_BUILD_DIR}"

EOF

# --- Return to main image ---
FROM main-build AS main

USER root

# Copy HAProxy binary from builder stage
COPY --from=haproxy-builder /usr/local/bin/haproxy /usr/local/bin/haproxy

# Install HAProxy runtime dependency and setup
RUN <<EOF
#!/bin/bash
set -euo pipefail

apt-get update -y
apt-get install -y --no-install-recommends socat liblua5.3-0

mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy
chown -R ray:"$(id -gn ray)" /run/haproxy

rm -rf /var/lib/apt/lists/*
EOF

USER ray
WORKDIR /home/ray

RUN mkdir -p /tmp/supervisord

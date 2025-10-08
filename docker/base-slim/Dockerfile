# syntax=docker/dockerfile:1.3-labs

# This Dockerfile is used to build the slim Ray image
# Mainly for use on Anyscale.

ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS main-build
ARG PYTHON_VERSION

RUN <<EOF
#!/bin/bash
set -euo pipefail
set -x

export DEBIAN_FRONTEND=noninteractive

APT_PKGS=(
	ca-certificates
	netbase
    tzdata
	curl
    sudo
    openssh-client
    openssh-server
    rsync
    zip
    unzip
    git
    gdb
    vim-tiny
    less
)

apt-get update
apt-get install -y --no-install-recommends "${APT_PKGS[@]}"
rm -rf /var/lib/apt/lists/*

useradd -ms /bin/bash -d /home/ray ray --uid 1000 --gid 100
usermod -aG sudo ray
echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers

# Install uv
curl -sSL -o- https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

# Determine the architecture of the host
if [[ "${HOSTTYPE}" =~ ^x86_64 ]]; then
    ARCH="x86_64"
elif [[ "${HOSTTYPE}" =~ ^aarch64 ]]; then
    ARCH="aarch64"
else
    echo "Unsupported architecture ${HOSTTYPE}" >/dev/stderr
    exit 1
fi

# Install dynolog
if [[ "$ARCH" == "x86_64" ]]; then
    DYNOLOG_TMP="$(mktemp -d)"
    (
        cd "${DYNOLOG_TMP}"
        curl -sSL https://github.com/facebookincubator/dynolog/releases/download/v0.3.2/dynolog_0.3.2-0-amd64.deb -o dynolog_0.3.2-0-amd64.deb
        sudo dpkg -i dynolog_0.3.2-0-amd64.deb
    )
    rm -rf "${DYNOLOG_TMP}"
fi

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

# Install awscli
AWSCLI_TMP="$(mktemp -d)"
(
    cd "${AWSCLI_TMP}"
    curl -sfL "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install
)
rm -rf "${AWSCLI_TMP}"
aws --version

EOF

# Switch to ray user
USER ray
ENV HOME=/home/ray
WORKDIR /home/ray

COPY python/requirements_compiled.txt /home/ray/requirements_compiled.txt

RUN <<EOF
#!/bin/bash
set -euo pipefail
set -x

MINIFORGE_VERSION="24.11.3-0"

# Install miniforge
MINIFORGE_LINK="https://github.com/conda-forge/miniforge/releases/download/${MINIFORGE_VERSION}/Miniforge3-${MINIFORGE_VERSION}-Linux-${HOSTTYPE}.sh"
curl -sfL -o /tmp/miniforge.sh "${MINIFORGE_LINK}"
bash /tmp/miniforge.sh -b -p /home/ray/anaconda3 # use anaconda3 to match existing images to avoid surprises.
rm /tmp/miniforge.sh

/home/ray/anaconda3/bin/conda init bash

eval "$(/home/ray/anaconda3/bin/conda shell.bash activate)"
/home/ray/anaconda3/bin/conda install -y "python=${PYTHON_VERSION}"
/home/ray/anaconda3/bin/conda clean -a

# Install python packages
PIP_PKGS=(
    anyscale
    packaging
    azure-identity
    "adlfs[abfs]"
    boto3
    google-cloud-storage
    jupyterlab
    ipywidgets
    supervisor
    "smart_open[s3,gcs,azure,http]"
)

uv pip install --system --no-cache-dir --index-strategy unsafe-best-match \
    -c /home/ray/requirements_compiled.txt \
    "${PIP_PKGS[@]}"
anyscale --version

mkdir -p /tmp/supervisord

EOF

ENV PATH="/home/ray/.local/bin:/home/ray/anaconda3/bin:$PATH"

# --- HAProxy Build Stage ---
FROM $BASE_IMAGE AS haproxy-builder

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
BUILD_DIR=$(mktemp -d)
curl -sSfL -o "${BUILD_DIR}/haproxy.tar.gz" "https://www.haproxy.org/download/2.8/src/haproxy-${HAPROXY_VERSION}.tar.gz"
tar -xzf "${BUILD_DIR}/haproxy.tar.gz" -C "${BUILD_DIR}" --strip-components=1
make -C "${BUILD_DIR}" TARGET=linux-glibc USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc)
make -C "${BUILD_DIR}" install SBINDIR=/usr/local/bin
rm -rf "${BUILD_DIR}"

EOF

# --- Return to main image ---
FROM main-build AS main

# Switch to root for system-level HAProxy setup
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

CMD ["bash"]

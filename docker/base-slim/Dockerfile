# syntax=docker/dockerfile:1.3-labs

# This Dockerfile is used to build the slim Ray image
# Mainly for use on Anyscale.

ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG PYTHON_VERSION="3.10"
ARG PYTHON_DEPSET="python/deplocks/base_slim/ray_base_slim_py${PYTHON_VERSION}.lock"
ARG CONSTRAINTS_FILE="python/requirements_compiled_py${PYTHON_VERSION}.txt"

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
apt-get upgrade -y
apt-get install -y --no-install-recommends "${APT_PKGS[@]}"
rm -rf /var/lib/apt/lists/*

useradd -ms /bin/bash -d /home/ray ray --uid 1000 --gid 100
usermod -aG sudo ray
echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers

# Install uv
curl -sSL -o- https://astral.sh/uv/0.13.4/install.sh | env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

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

COPY "$CONSTRAINTS_FILE" /home/ray/requirements_compiled.txt
COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

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
# conda-forge certifi builds from 2026 on require python>=3.11. Downgrading the
# Miniforge python to 3.10 without a certifi cap makes the solver drop certifi,
# requests and conda itself, and the transaction fails with RemoveError.
# conda-forge pip 26.2.2 (the eMudhra CA fix) also requires python>=3.11, so
# 3.10 installs it from PyPI after the conda step.
# TODO(elliot-barn): remove this conditional once Python 3.10 is deprecated.
CONDA_PYTHON_EXTRA_SPECS=("pip=26.2.2")
if [[ "${PYTHON_VERSION}" == "3.10" ]]; then
    CONDA_PYTHON_EXTRA_SPECS=("certifi<2026")
fi
/home/ray/anaconda3/bin/conda install -y "python=${PYTHON_VERSION}" "${CONDA_PYTHON_EXTRA_SPECS[@]}"
/home/ray/anaconda3/bin/conda clean -a

# conda-forge pip 26.2.2 requires python>=3.11; PyPI's supports 3.10.
if [[ "${PYTHON_VERSION}" == "3.10" ]]; then
    uv pip install --system --no-cache-dir pip==26.2.2
fi

uv pip install --system --no-cache-dir --no-deps --index-strategy unsafe-best-match \
    -r $HOME/python_depset.lock

anyscale --version

mkdir -p /tmp/supervisord

EOF

ENV PATH="/home/ray/.local/bin:/home/ray/anaconda3/bin:$PATH"

CMD ["bash"]

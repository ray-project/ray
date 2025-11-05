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
ARG PYTHON_VERSION=3.9

ARG RAY_UID=1000
ARG RAY_GID=100

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -y
apt-get upgrade -y

APT_PKGS=(
    sudo
    tzdata
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
)

apt-get install -y "${APT_PKGS[@]}"

useradd -ms /bin/bash -d /home/ray ray --uid $RAY_UID --gid $RAY_GID
usermod -aG sudo ray
echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers

EOF

USER $RAY_UID
ENV HOME=/home/ray
WORKDIR /home/ray

COPY python/requirements_compiled.txt /home/ray/requirements_compiled.txt

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

PIP_PKGS=(
    # Required a recent version of setuptools to be compatible with python 3.12+.
    setuptools==80.9.0

    flatbuffers
    cython
    numpy  # Necessary for Dataset to work properly.
    psutil

    # For the ease to submit jobs on various cloud providers.
    "smart_open[s3,gcs,azure,http]"

    six
    boto3
    pyopenssl
    cryptography
    google-api-python-client
    google-oauth
    "adlfs[abfs]"
)

# Install uv
wget -qO- https://astral.sh/uv/install.sh | sudo -E env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

# Set up Conda as system Python
export PATH=$HOME/anaconda3/bin:$PATH

# Some packages are on PyPI as well as other indices, but the latter
# (unhelpfully) take precedence. We use `--index-strategy unsafe-best-match`
# to ensure that the best match is chosen from the available indices.
uv pip install --system --no-cache-dir --index-strategy unsafe-best-match \
    -c $HOME/requirements_compiled.txt \
    "${PIP_PKGS[@]}"

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

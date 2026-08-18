# syntax=docker/dockerfile:1.3-labs
ARG BASE_IMAGE=nvidia/cuda:12.8.1-cudnn-devel-ubuntu22.04
FROM $BASE_IMAGE

ARG BUILDKITE_BAZEL_CACHE_URL
ARG PYTHON=3.10
ARG CUDA_VERSION=12.8.1

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles

ENV RAY_BUILD_ENV=ubuntu22.04_clang14_cuda${CUDA_VERSION}_py$PYTHON
ENV BUILDKITE=true
ENV CI=true
ENV PYTHON=$PYTHON
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV RAY_INSTALL_JAVA=0
ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}

# See ci/docker/base.test.Dockerfile: retry transient files.pythonhosted.org failures.
ENV PIP_RETRIES=9
ENV UV_HTTP_RETRIES=9

# Where pip and uv resolve from during this build. ci/pypi_proxy_profile.sh sets
# RAYCI_IMAGE_PIP_INDEX_URL to the CI package mirror when the job can reach it and
# leaves it empty otherwise, which is also the case for any build outside CI, and
# then this is the index pip would have used anyway. Kept as one build arg with a
# default rather than a bare ARG so the value stays stable across jobs: it is part
# of the wanda cache key, and a per-job value would rebuild every image every build.
ARG RAYCI_IMAGE_PIP_INDEX_URL=""
ENV PIP_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}
ENV UV_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -qq && apt-get upgrade -qq
apt-get install -y -qq \
    curl python-is-python3 git build-essential \
    sudo zip unzip unrar apt-utils dialog tzdata wget rsync \
    language-pack-en tmux cmake gdb vim htop \
    libgtk2.0-dev zlib1g-dev libgl1-mesa-dev \
    clang-format-14 jq \
    clang-tidy-14 clang-14
ln -s /usr/bin/clang-format-14 /usr/bin/clang-format
ln -s /usr/bin/clang-tidy-14 /usr/bin/clang-tidy
ln -s /usr/bin/clang-14 /usr/bin/clang

# Install docker CLI
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce-cli

echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}" >> /root/.bazelrc

curl -fsSL https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

EOF

ENV CC=clang
ENV CXX=clang++-14

# System conf for tests
RUN locale -a
ENV LC_ALL=en_US.utf8
ENV LANG=en_US.utf8
RUN echo "ulimit -c 0" >> /root/.bashrc

# Install some dependencies (miniforge, pip dependencies, etc)
RUN mkdir /ray
WORKDIR /ray

COPY . .

RUN bash --login -ie -c '\
    BUILD=1 SKIP_PYTHON_PACKAGES=1 ./ci/env/install-dependencies.sh \
'

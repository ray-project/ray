# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:jammy

ARG BUILDKITE_BAZEL_CACHE_URL
ARG PYTHON=3.10

ENV DEBIAN_FRONTEND=noninteractive

# Where pip and uv resolve from while building this image. Docker builds cannot see an
# index configured in the CI step's environment -- BuildKit RUN steps inherit nothing
# from it -- so it arrives as a build arg, which wanda resolves from
# RAYCI_IMAGE_PIP_INDEX_URL in the job environment.
#
# Empty for anyone building these images outside CI, and then this is exactly the index
# pip would have used anyway, so an external build behaves as it does today.
ARG RAYCI_IMAGE_PIP_INDEX_URL=""
ENV PIP_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}
ENV UV_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}

# pip refuses a plain-HTTP index unless the host is named as trusted, with loopback the
# one exemption -- and this address is a name, not loopback. The refusal is silent: the
# index is dropped and the install fails with "from versions: none" rather than a
# connection error (release 104844, cython==3.0.12 in the wheel build). Arrives the same
# way as the index above and is empty outside CI, where the index is public PyPI over
# HTTPS and there is nothing to trust.
ARG RAYCI_IMAGE_PIP_TRUSTED_HOST=""
ENV PIP_TRUSTED_HOST=${RAYCI_IMAGE_PIP_TRUSTED_HOST}
ENV UV_INSECURE_HOST=${RAYCI_IMAGE_PIP_TRUSTED_HOST}
ENV TZ=America/Los_Angeles

ENV RAY_BUILD_ENV=ubuntu22.04_clang14_py$PYTHON
ENV BUILDKITE=true
ENV CI=true
ENV PYTHON=$PYTHON
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV RAY_INSTALL_JAVA=0
ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -qq && apt-get upgrade -qq
apt-get install -y -qq \
    curl python-is-python3 git build-essential \
    sudo zip unzip unrar apt-utils dialog tzdata wget rsync \
    language-pack-en tmux cmake gdb vim htop graphviz \
    libgtk2.0-dev zlib1g-dev libgl1-mesa-dev \
    liblz4-dev libunwind-dev libncurses6 \
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

RUN ./ci/env/install-miniforge.sh
RUN ./ci/env/install-bazel.sh

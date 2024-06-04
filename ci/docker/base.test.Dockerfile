# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:focal

ARG BUILDKITE_BAZEL_CACHE_URL
ARG PYTHON=3.12

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles

ENV BUILDKITE=true
ENV CI=true
ENV PYTHON=$PYTHON
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV RAY_INSTALL_JAVA=0
# For wheel build
# https://github.com/docker-library/docker/blob/master/20.10/docker-entrypoint.sh
ENV DOCKER_TLS_CERTDIR=/certs
ENV DOCKER_HOST=tcp://docker:2376
ENV DOCKER_TLS_VERIFY=1
ENV DOCKER_CERT_PATH=/certs/client
ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}

RUN <<EOF
#!/bin/bash

apt-get update -qq && apt-get upgrade -qq
apt-get install -y -qq \
    curl python-is-python3 git build-essential \
    sudo unzip unrar apt-utils dialog tzdata wget rsync \
    language-pack-en tmux cmake gdb vim htop graphviz \
    libgtk2.0-dev zlib1g-dev libgl1-mesa-dev \
    liblz4-dev libunwind-dev libncurses5 \
    clang-format-12 jq \
    clang-tidy-12 clang-12

ln -s /usr/bin/clang-format-12 /usr/bin/clang-format
ln -s /usr/bin/clang-tidy-12 /usr/bin/clang-tidy
ln -s /usr/bin/clang-12 /usr/bin/clang

EOF

RUN curl -o- https://get.docker.com | sh

# System conf for tests
RUN locale -a
ENV LC_ALL=en_US.utf8
ENV LANG=en_US.utf8
RUN echo "ulimit -c 0" >> /root/.bashrc

# Install some dependencies (miniconda, pip dependencies, etc)
RUN mkdir /ray
WORKDIR /ray

# Below should be re-run each time
COPY . .

RUN ./ci/env/install-bazel.sh

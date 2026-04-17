# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray:nightly"$BASE_IMAGE"
FROM "$FULL_BASE_IMAGE"
ARG PYTHON_VERSION=3.13
ARG IMAGE_TYPE=ray-torch-cpu
ARG PYTHON_DEPSET=python/deplocks/ray-torch/${IMAGE_TYPE}_base_py${PYTHON_VERSION}.lock

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

set -x
alias pip="$HOME/anaconda3/bin/pip"

sudo apt-get update \
    && sudo apt-get install -y gcc \
        cmake \
        libgtk2.0-dev \
        libgl1-mesa-dev \
        libgl1-mesa-glx \
        libosmesa6 \
        libosmesa6-dev \
        libglfw3 \
        patchelf \
        unzip \
        unrar \
        zlib1g-dev

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

EOF

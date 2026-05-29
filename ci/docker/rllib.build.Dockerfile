# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAYCI_IS_GPU_BUILD
ARG PYTHON
ARG BUILD_VARIANT=build
ARG PYTHON_DEPSET=python/deplocks/ci/rllib-$BUILD_VARIANT-ci_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

# Install system packages required for gymnasium rendering (MuJoCo, OpenGL, GLFW)
sudo apt-get update -qq && sudo apt-get install -y --no-install-recommends \
    libosmesa6-dev libgl1 libglfw3 patchelf
rm -rf /var/lib/apt/lists/*

# Install Python dependencies from depset lock file
uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

EOF

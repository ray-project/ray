# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_gpu-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON
ARG PYTHON_DEPSET=python/deplocks/ci/docgpu_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

# Install ray from PyPI to get matching compiled binaries (_raylet.so)
# The depset excludes ray (--unsafe-package ray) since it uses a placeholder wheel
pip install "ray>=2.47.1"

EOF

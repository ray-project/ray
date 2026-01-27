# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ENABLE_TRACING
ARG PYDANTIC_VERSION
ARG IMAGE_TYPE="base"
ARG PYTHON
ARG PYTHON_DEPSET="python/deplocks/ci/serve_{IMAGE_TYPE}_depset_py${PYTHON}.lock"

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

uv pip install --system --no-cache-dir --no-deps --index-strategy unsafe-best-match \
    -r /home/ray/python_depset.lock

git clone --branch=4.2.0 --depth=1 https://github.com/wg/wrk.git /tmp/wrk
make -C /tmp/wrk -j
sudo cp /tmp/wrk/wrk /usr/local/bin/wrk
rm -rf /tmp/wrk

EOF

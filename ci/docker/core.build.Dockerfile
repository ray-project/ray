ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON=3.10
ARG BUILD_VARIANT=build
ARG PYTHON_DEPSET=python/deplocks/ci/core-${BUILD_VARIANT}-ci_depset_py${PYTHON}.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

EOF

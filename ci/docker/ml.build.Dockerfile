# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAYCI_IS_GPU_BUILD=false
ARG RAYCI_LIGHTNING_2=false
ARG PYTHON
ARG BUILD_VARIANT=build
ARG PYTHON_DEPSET=python/deplocks/ci/ml-$BUILD_VARIANT-ci_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

set -x

./ci/env/install-hdfs.sh

# Install HEBO for testing (not supported on Python 3.12+)
if [[ "${PYTHON-}" != "3.12" ]]; then
  pip install HEBO==0.3.5
fi

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

EOF

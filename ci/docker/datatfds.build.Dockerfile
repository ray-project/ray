# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_VERSION=14.*
ARG ARROW_MONGO_VERSION=
ARG RAY_CI_JAVA_BUILD=
ARG PYTHON=3.12
ARG PYTHON_DEPSET=python/deplocks/tests/data/datatfds_py$PYTHON.lock
SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

EOF

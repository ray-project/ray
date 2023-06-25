#!/bin/bash

set -euo pipefail

export DOCKER_BUILDKIT=1

PYTHON_VERSION_NAME=py310
PYTHON_VERSION="3.10"

tar --mtime="UTC 2020-01-01" -c -f - docker/base-deps/Dockerfile \
    | docker build --progress=plain \
        -t "ray-project/base-deps:${PYTHON_VERSION_NAME}" \
        --build-arg "BASE_IMAGE=ubuntu:focal" \
        --build-arg "PYTHON_VERSION=${PYTHON_VERSION}" \
        --build-arg "HOSTTYPE=${HOSTTYPE}" \
        -f docker/base-deps/Dockerfile -

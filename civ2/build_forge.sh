#!/bin/bash

set -euxo pipefail

export DOCKER_BUILDKIT=1

readonly IMAGE_NAME="quay.io/pypa/manylinux2014_x86_64"
readonly IMAGE_TAG="2022-12-20-b4884d9"
tar --mtime="UTC 2020-01-01" -c -f - civ2/rayforge/Dockerfile \
    | docker build --progress=plain -t rayforge \
        --build-arg "BASE_IMAGE=${IMAGE_NAME}:${IMAGE_TAG}" \
        -f civ2/rayforge/Dockerfile -

#!/bin/bash

set -euo pipefail

export DOCKER_BUILDKIT=1
PYTHON_VERSION_CODE="$1"
IMAGE_TYPE="$2"

if [[ "${PYTHON_VERSION_CODE}" == "py37" ]]; then
    PYTHON_VERSION="3.7"
elif [[ "${PYTHON_VERSION_CODE}" == "py38" ]]; then
    PYTHON_VERSION="3.8"
elif [[ "${PYTHON_VERSION_CODE}" == "py39" ]]; then
    PYTHON_VERSION="3.9"
elif [[ "${PYTHON_VERSION_CODE}" == "py310" ]]; then
    PYTHON_VERSION="3.10"
elif [[ "${PYTHON_VERSION_CODE}" == "py311" ]]; then
    PYTHON_VERSION="3.11"
else
    echo "Unknown python version code: ${PYTHON_VERSION_CODE}" >/dev/stderr
    exit 1
fi

# Needs to sync with ci/build/build-docker-images.py
if [[ "${IMAGE_TYPE}" == "cpu" ]]; then
    BASE_IMAGE="ubuntu:focal"
elif [[ "${IMAGE_TYPE}" == "gpu" ]]; then
    # TODO(can): de-duplicate with cu118
    BASE_IMAGE="nvidia/cuda:11.8.0-cudnn8-devel-ubuntu20.04"
elif [[ "${IMAGE_TYPE}" == "cu115" ]]; then
    BASE_IMAGE="nvidia/cuda:11.5.2-cudnn8-devel-ubuntu20.04"
elif [[ "${IMAGE_TYPE}" == "cu116" ]]; then
    BASE_IMAGE="nvidia/cuda:11.6.2-cudnn8-devel-ubuntu20.04"
elif [[ "${IMAGE_TYPE}" == "cu117" ]]; then
    BASE_IMAGE="nvidia/cuda:11.7.1-cudnn8-devel-ubuntu20.04"
elif [[ "${IMAGE_TYPE}" == "cu118" ]]; then
    BASE_IMAGE="nvidia/cuda:11.8.0-cudnn8-devel-ubuntu20.04"
elif [[ "${IMAGE_TYPE}" == "cu121" ]]; then
    BASE_IMAGE="nvidia/cuda:12.1.1-cudnn8-devel-ubuntu20.04"
else
    echo "Unknown image type: ${IMAGE_TYPE}" >/dev/stderr
    exit 1
fi

echo "--- Build base for ${PYTHON_VERSION_CODE} ${IMAGE_TYPE}"

DEST_IMAGE="${CI_TMP_REPO}:${IMAGE_PREFIX}-base-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"

tar --mtime="UTC 2020-01-01" -c -f - \
    docker/base-deps/Dockerfile \
    | docker build --progress=plain \
        --build-arg BASE_IMAGE="${BASE_IMAGE}" \
        --build-arg PYTHON_VERSION="${PYTHON_VERSION}" \
        -t "${DEST_IMAGE}" -f docker/base-deps/Dockerfile -

docker push "${DEST_IMAGE}"

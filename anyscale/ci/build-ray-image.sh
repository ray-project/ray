#!/bin/bash

set -euo pipefail

PY_VERSION="${1:-3.8}"
IMG_TYPE="${2:-cpu}"
BASE_TYPE="${3:-ray}"

RAY_VERSION="3.0.0.dev0"
S3_TEMP="s3://bk-premerge-first-jawfish-artifacts/tmp/runtime/${RAYCI_BUILD_ID}"
RUNTIME_ECR="830883877497.dkr.ecr.us-west-2.amazonaws.com"
IMAGE_PREFIX="${RAYCI_BUILD_ID}"

# Default cuda is also tagged as "gpu".
readonly ML_CUDA_VERSION="cu118"

# Always use buildkit.
export DOCKER_BUILDKIT=1

if [[ "${BASE_TYPE}" == "ray" ]]; then
    RUNTIME_REPO="830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/runtime"
elif [[ "${BASE_TYPE}" == "ray-ml" ]]; then
    RUNTIME_REPO="830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/runtime-ml"
else
    echo "Unknown base type: ${BASE_TYPE}" > /dev/stderr
    exit 1
fi

if [[ "$(uname -m)" == "x86_64" ]]; then
    HOSTTYPE="x86_64"
    IMG_SUFFIX=""
else
    HOSTTYPE="aarch64"
    IMG_SUFFIX="-aarch64"
fi

if [[ "${PY_VERSION}" == "3.8" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp38-cp38-manylinux2014_${HOSTTYPE}.whl"
    PY_VERSION_CODE="py38"
elif [[ "${PY_VERSION}" == "3.9" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp39-cp39-manylinux2014_${HOSTTYPE}.whl"
    PY_VERSION_CODE="py39"
elif [[ "${PY_VERSION}" == "3.10" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp310-cp310-manylinux2014_${HOSTTYPE}.whl"
    PY_VERSION_CODE="py310"
elif [[ "${PY_VERSION}" == "3.11" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp311-cp311-manylinux2014_${HOSTTYPE}.whl"
    PY_VERSION_CODE="py311"
else
    echo "Unknown python version code: ${PY_VERSION}" >/dev/stderr
    exit 1
fi

if [[ "${IMG_TYPE}" == "cpu" ]]; then
    IMG_TYPE_CODE=cpu
elif [[ "${IMG_TYPE}" == "cu11.5.2" ]]; then
    IMG_TYPE_CODE="cu115"
elif [[ "${IMG_TYPE}" == "cu11.6.2" ]]; then
    IMG_TYPE_CODE="cu116"
elif [[ "${IMG_TYPE}" == "cu11.7.1" ]]; then
    IMG_TYPE_CODE="cu117"
elif [[ "${IMG_TYPE}" == "cu11.8.0" ]]; then
    IMG_TYPE_CODE="cu118"
elif [[ "${IMG_TYPE}" == "cu12.1.1" ]]; then
    IMG_TYPE_CODE="cu121"
else
    echo "Unknown image type: ${IMG_TYPE}" >/dev/stderr
    exit 1
fi

function docker_push_as {
    local SRC_IMG="$1"
    local DEST_IMG="$2"
    docker tag "${SRC_IMG}" "${DEST_IMG}"
    docker push "${DEST_IMG}"
}

function docker_push {
    local IMG="$1"
    docker push "${IMG}"
}

if [[ "${PUSH_COMMIT_TAGS:-}" == "" ]]; then
    if [[ "${BUILDKITE_BRANCH:-}" =~ ^(master|releases/) ]]; then
        PUSH_COMMIT_TAGS="true"
    else
        PUSH_COMMIT_TAGS="false"
    fi
fi

BUILD_TMP="$(mktemp -d)"

mkdir -p "${BUILD_TMP}/.whl"

FULL_COMMIT="$(git rev-parse HEAD)"
SHORT_COMMIT="${FULL_COMMIT:0:6}"  # Use 6 chars to be consistent with Ray upstream
# During branch cut, do not modify ray version in this script
if [[ ! "${RAY_VERSION:-}" =~ dev ]]; then
    SHORT_COMMIT="${RAY_VERSION}.${SHORT_COMMIT}"
fi

echo "--- Fetch wheel and base image"

aws s3 cp "${S3_TEMP}/${WHEEL_FILE}" "${BUILD_TMP}/.whl/${WHEEL_FILE}"
aws s3 cp "s3://runtime-release-test-artifacts/dataplane/20230831/dataplane.tar.gz" "${BUILD_TMP}/dataplane.tar.gz"
DATAPLANE_TGZ_GOT="$(sha256sum "${BUILD_TMP}/dataplane.tar.gz" | cut -d' ' -f1)"
DATAPLANE_TGZ_WANT="3658592a3b30a93b2e940269f99296e822446eed7ef36049d707b435ff868638"
if [[ "${DATAPLANE_TGZ_GOT}" != "${DATAPLANE_TGZ_WANT}" ]]; then
    echo "Dataplane tarball sha256 digest:" \
        "got ${DATAPLANE_TGZ_GOT}, want ${DATAPLANE_TGZ_WANT}" >/dev/stderr
    exit 1
fi

BASE_IMG="${RAYCI_WORK_REPO}:${IMAGE_PREFIX}-ray-py${PY_VERSION}-${IMG_TYPE}-base"

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "${RUNTIME_ECR}"

docker pull "${BASE_IMG}"

# Everything is prepared, starts building now.

export DOCKER_BUILDKIT=1

BUILD_TAG="${IMAGE_PREFIX}-${PY_VERSION_CODE}-${IMG_TYPE_CODE}${IMG_SUFFIX}"
RAY_IMG="${RUNTIME_REPO}:${BUILD_TAG}"
ANYSCALE_IMG="${RUNTIME_REPO}:${BUILD_TAG}-as"

echo "--- Build ${RAY_IMG}"

CONTEXT_TMP="$(mktemp -d)"

mkdir -p "${CONTEXT_TMP}/.whl"

cp "${BUILD_TMP}/.whl/${WHEEL_FILE}" "${CONTEXT_TMP}/.whl/${WHEEL_FILE}"
cp docker/ray/Dockerfile "${CONTEXT_TMP}/Dockerfile"
cp python/requirements_compiled.txt "${CONTEXT_TMP}/."

CONSTRAINTS_FILE="requirements_compiled.txt"

(
    cd "${CONTEXT_TMP}"
    tar --mtime="UTC 2020-01-01" -c -f - . \
        | docker build --progress=plain \
            --build-arg FULL_BASE_IMAGE="${BASE_IMG}" \
            --build-arg WHEEL_PATH=".whl/${WHEEL_FILE}" \
            --build-arg CONSTRAINTS_FILE="${CONSTRAINTS_FILE}" \
            -t "${RAY_IMG}" -f Dockerfile -
)

echo "--- Build ${ANYSCALE_IMG}"
docker build --progress=plain \
    --build-arg BASE_IMAGE="${RAY_IMG}" \
    -t "${ANYSCALE_IMG}" -f Dockerfile - < "${BUILD_TMP}/dataplane.tar.gz"


echo "--- Pushing images"

docker_push "${RAY_IMG}"
docker_push "${ANYSCALE_IMG}"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    COMMIT_TAG="${SHORT_COMMIT}-${PY_VERSION_CODE}-${IMG_TYPE_CODE}${IMG_SUFFIX}"
    docker_push_as "${RAY_IMG}" "${RUNTIME_REPO}:${COMMIT_TAG}"
    docker_push_as "${ANYSCALE_IMG}" "${RUNTIME_REPO}:${COMMIT_TAG}-as"
    
    if [[ "${IMG_TYPE_CODE}" == "${ML_CUDA_VERSION}" ]]; then
        COMMIT_GPU_TAG="${SHORT_COMMIT}-${PY_VERSION_CODE}-gpu${IMG_SUFFIX}"
        docker_push_as "${RAY_IMG}" "${RUNTIME_REPO}:${COMMIT_GPU_TAG}"
        docker_push_as "${ANYSCALE_IMG}" "${RUNTIME_REPO}:${COMMIT_GPU_TAG}-as"
    fi
fi

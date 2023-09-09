#!/bin/bash

set -euo pipefail

PY_VERSION_CODE="$1"
IMG_TYPE="$2"
: "${RAY_VERSION:=3.0.0.dev0}"

function docker_push_as {
    local SRC_IMG="$1"
    local DEST_IMG="$2"
    docker tag "${SRC_IMG}" "${DEST_IMG}"
    docker push "${DEST_IMG}"
    buildkite-agent annotate --style=info \
        --context="${PY_VERSION_CODE}-images" --append "${DEST_IMG}<br/>"
}

function docker_push {
    local IMG="$1"
    docker push "${IMG}"
    buildkite-agent annotate --style=info \
        --context="${PY_VERSION_CODE}-images" --append "${IMG}<br/>"
}

readonly ML_CUDA_VERSION="cu118"

if [[ "$(uname -m)" == "x86_64" ]]; then
    HOSTTYPE="x86_64"
    IMG_SUFFIX=""
else
    HOSTTYPE="aarch64"
    IMG_SUFFIX="-aarch64"
fi

if [[ "${PY_VERSION_CODE}" == "py37" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp37-cp37m-manylinux2014_${HOSTTYPE}.whl"
elif [[ "${PY_VERSION_CODE}" == "py38" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp38-cp38-manylinux2014_${HOSTTYPE}.whl"
elif [[ "${PY_VERSION_CODE}" == "py39" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp39-cp39-manylinux2014_${HOSTTYPE}.whl"
elif [[ "${PY_VERSION_CODE}" == "py310" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp310-cp310-manylinux2014_${HOSTTYPE}.whl"
elif [[ "${PY_VERSION_CODE}" == "py311" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp311-cp311-manylinux2014_${HOSTTYPE}.whl"
else
    echo "Unknown python version code: ${PY_VERSION_CODE}" >/dev/stderr
    exit 1
fi

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

BASE_IMG="${CI_TMP_REPO}:${IMAGE_PREFIX}-base-${PY_VERSION_CODE}-${IMG_TYPE}${IMG_SUFFIX}"

docker pull "${BASE_IMG}"
# This retagging is required because the Dockerfile hardcodes the base image.
docker tag "${BASE_IMG}" "rayproject/ray-deps:nightly-${PY_VERSION_CODE}-${IMG_TYPE}"

# Everything is prepared, starts building now.

export DOCKER_BUILDKIT=1

BUILD_TAG="${IMAGE_PREFIX}-${PY_VERSION_CODE}-${IMG_TYPE}${IMG_SUFFIX}"
COMMIT_TAG="${SHORT_COMMIT}-${PY_VERSION_CODE}-${IMG_TYPE}${IMG_SUFFIX}"
COMMIT_GPU_TAG="${SHORT_COMMIT}-${PY_VERSION_CODE}-gpu${IMG_SUFFIX}"
RAY_IMG="${RUNTIME_REPO}:${BUILD_TAG}"
ANYSCALE_IMG="${RUNTIME_REPO}:${BUILD_TAG}-as"

echo "--- Build ${RAY_IMG}"

CPU_TMP="$(mktemp -d)"

mkdir -p "${CPU_TMP}/.whl"

cp "${BUILD_TMP}/.whl/${WHEEL_FILE}" "${CPU_TMP}/.whl/${WHEEL_FILE}"
cp docker/ray/Dockerfile "${CPU_TMP}/Dockerfile"
cp python/requirements_compiled.txt "${CPU_TMP}/."
cp python/requirements_compiled_py37.txt "${CPU_TMP}/."

if [[ "${PY_VERSION_CODE}" == "py37" ]]; then
    CONSTRAINTS_FILE="requirements_compiled_py37.txt"
else
    CONSTRAINTS_FILE="requirements_compiled.txt"
fi

(
    cd "${CPU_TMP}"
    tar --mtime="UTC 2020-01-01" -c -f - . \
        | docker build --progress=plain \
            --build-arg BASE_IMAGE="-${PY_VERSION_CODE}-${IMG_TYPE}" \
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
    docker_push_as "${RAY_IMG}" "${RUNTIME_REPO}:${COMMIT_TAG}"
    docker_push_as "${ANYSCALE_IMG}" "${RUNTIME_REPO}:${COMMIT_TAG}-as"
    if [[ "${IMG_TYPE}" == "${ML_CUDA_VERSION}" ]]; then
        docker_push_as "${RAY_IMG}" "${RUNTIME_REPO}:${COMMIT_GPU_TAG}"
        docker_push_as "${ANYSCALE_IMG}" "${RUNTIME_REPO}:${COMMIT_GPU_TAG}-as"
    fi
fi

# This retagging is required because the Dockerfile hardcodes the base image.
docker tag "${RAY_IMG}" "rayproject/ray:nightly-${PY_VERSION_CODE}-${IMG_TYPE}${IMG_SUFFIX}"

if [[ "${PY_VERSION_CODE}" == "py37" || "${PY_VERSION_CODE}" == "py311" || "${HOSTTYPE}" == "aarch64" ]]; then
    echo "Skipping ML image for ${PY_VERSION_CODE}" and host ${HOSTTYPE}
    exit 0
fi

# Build and push the ML image.
RAY_ML_IMG="${RUNTIME_ML_REPO}:${BUILD_TAG}"
ANYSCALE_ML_IMG="${RUNTIME_ML_REPO}:${BUILD_TAG}-as"
echo "--- Build ${RAY_ML_IMG}"

ML_TMP="$(mktemp -d)"

cp docker/ray-ml/Dockerfile "${ML_TMP}/Dockerfile"
cp docker/ray-ml/install-ml-docker-requirements.sh "${ML_TMP}/."
cp python/requirements.txt "${ML_TMP}/."
cp python/requirements_compiled.txt "${ML_TMP}/."
cp python/requirements/docker/ray-docker-requirements.txt "${ML_TMP}/."
cp python/requirements/ml/*-requirements.txt "${ML_TMP}/."

(
    cd "${ML_TMP}"
    tar --mtime="UTC 2020-01-01" -c -f - . \
        | docker build --progress=plain \
            --build-arg BASE_IMAGE="-${PY_VERSION_CODE}-${IMG_TYPE}" \
            -t "${RAY_ML_IMG}" -f Dockerfile -
)

echo "--- Build ${ANYSCALE_ML_IMG}"
docker build --progress=plain \
    --build-arg BASE_IMAGE="${RAY_ML_IMG}" \
    -t "${ANYSCALE_ML_IMG}" -f Dockerfile - < "${BUILD_TMP}/dataplane.tar.gz"

echo "--- Pushing ML images"
docker_push "${RAY_ML_IMG}"
docker_push "${ANYSCALE_ML_IMG}"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    docker_push_as "${RAY_ML_IMG}" "${RUNTIME_ML_REPO}:${COMMIT_TAG}"
    docker_push_as "${ANYSCALE_ML_IMG}" "${RUNTIME_ML_REPO}:${COMMIT_TAG}-as"

    if [[ "${IMG_TYPE}" == "${ML_CUDA_VERSION}" ]]; then
        docker_push_as "${RAY_ML_IMG}" "${RUNTIME_ML_REPO}:${COMMIT_GPU_TAG}"
        docker_push_as "${ANYSCALE_ML_IMG}" "${RUNTIME_ML_REPO}:${COMMIT_TAG}-as"
    fi
fi

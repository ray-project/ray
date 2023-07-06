#!/bin/bash

set -euo pipefail

PYTHON_VERSION_CODE="$1"
IMAGE_TYPE="$2"

if [[ "${PYTHON_VERSION_CODE}" == "py37" ]]; then
    WHEEL_FILE="ray-3.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl"
elif [[ "${PYTHON_VERSION_CODE}" == "py38" ]]; then
    WHEEL_FILE="ray-3.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl"
elif [[ "${PYTHON_VERSION_CODE}" == "py39" ]]; then
    WHEEL_FILE="ray-3.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl"
elif [[ "${PYTHON_VERSION_CODE}" == "py310" ]]; then
    WHEEL_FILE="ray-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl"
elif [[ "${PYTHON_VERSION_CODE}" == "py311" ]]; then
    WHEEL_FILE="ray-3.0.0.dev0-cp311-cp311-manylinux2014_x86_64.whl"
else
    echo "Unknown python version code: ${PYTHON_VERSION_CODE}" >/dev/stderr
    exit 1
fi

if [[ "${PUSH_COMMIT_TAGS:-}" == "" ]]; then
    if [[ "${BUILDKITE_BRANCH:-}" == "master" ]]; then
        PUSH_COMMIT_TAGS="true"
    else
        PUSH_COMMIT_TAGS="false"
    fi
fi

mkdir -p .whl

FULL_COMMIT="$(git rev-parse HEAD)"
SHORT_COMMIT="${FULL_COMMIT:0:6}"  # Use 6 chars to be consistent with Ray upstream

echo "--- Fetch wheel and base image"

aws s3 cp "${S3_TEMP}/${WHEEL_FILE}" ".whl/${WHEEL_FILE}"

BASE_IMAGE="${CI_TMP_REPO}:${IMAGE_PREFIX}-base-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"

docker pull "${BASE_IMAGE}"
# This retagging is required because the Dockerfile hardcodes the base image.
docker tag "${BASE_IMAGE}" "rayproject/ray-deps:nightly-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"

DEST_IMAGE="${RUNTIME_REPO}:${IMAGE_PREFIX}-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"

echo "--- Build ${DEST_IMAGE}"

tar --mtime="UTC 2020-01-01" -c -f - \
    ".whl/${WHEEL_FILE}" \
    docker/ray/Dockerfile \
    | docker build --progress=plain \
        --build-arg BASE_IMAGE="-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}" \
        --build-arg WHEEL_PATH=".whl/${WHEEL_FILE}" \
        -t "${DEST_IMAGE}" -f docker/ray/Dockerfile -

docker push "${DEST_IMAGE}"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    DEST_COMMIT_IMAGE="${RUNTIME_REPO}:${SHORT_COMMIT}-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"
    docker tag "${DEST_IMAGE}" "${DEST_COMMIT_IMAGE}"
    docker push "${DEST_COMMIT_IMAGE}"
fi

DEST_ML_IMAGE="${RUNTIME_ML_REPO}:${IMAGE_PREFIX}-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"
echo "--- Build ${DEST_ML_IMAGE}"

ML_TMP="$(mktemp -d)"

cp docker/ray-ml/Dockerfile "${ML_TMP}/Dockerfile"
cp docker/ray-ml/install-ml-docker-requirements.sh "${ML_TMP}/."
cp python/requirements.txt "${ML_TMP}/."
cp python/requirements/docker/ray-docker-requirements.txt "${ML_TMP}/."
cp python/requirements/ml/*-requirements.txt "${ML_TMP}/."
# This retagging is required because the Dockerfile hardcodes the base image.
docker tag "${DEST_IMAGE}" "rayproject/ray:nightly-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"

(
    cd "${ML_TMP}"
    tar --mtime="UTC 2020-01-01" -c -f - . \
        | docker build --progress=plain \
            --build-arg BASE_IMAGE="-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}" \
            -t "${DEST_ML_IMAGE}" -f Dockerfile -
)

docker push "${DEST_ML_IMAGE}"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    DEST_COMMIT_ML_IMAGE="${RUNTIME_ML_REPO}:${SHORT_COMMIT}-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"
    docker tag "${DEST_ML_IMAGE}" "${DEST_COMMIT_ML_IMAGE}"
    docker push "${DEST_COMMIT_ML_IMAGE}"
fi

buildkite-agent annotate --style=info \
    --context="${PYTHON_VERSION_CODE}-images" \
    --append "Image: ${DEST_IMAGE}<br/>"
buildkite-agent annotate --style=info \
    --context="${PYTHON_VERSION_CODE}-images" \
    --append "ML image: ${DEST_ML_IMAGE}<br/>"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    buildkite-agent annotate --style=info \
        --context="${PYTHON_VERSION_CODE}-images" \
        --append "Image (commit tagged): ${DEST_COMMIT_IMAGE}<br/>"
    buildkite-agent annotate --style=info \
        --context="${PYTHON_VERSION_CODE}-images" \
        --append "ML image (commit tagged): ${DEST_COMMIT_ML_IMAGE}<br/>"
fi

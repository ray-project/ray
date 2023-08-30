#!/bin/bash

set -euo pipefail

PYTHON_VERSION_CODE="$1"
IMAGE_TYPE="$2"
ML_CUDA_VERSION="cu118"

if [[ "$(uname -m)" == "x86_64" ]]; then
    HOSTTYPE="x86_64"
    IMAGE_SUFFIX=""
else
    HOSTTYPE="aarch64"
    IMAGE_SUFFIX="-aarch64"
fi


RAY_VERSION="3.0.0.dev0"

if [[ "${PYTHON_VERSION_CODE}" == "py37" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp37-cp37m-manylinux2014_${HOSTTYPE}.whl"
elif [[ "${PYTHON_VERSION_CODE}" == "py38" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp38-cp38-manylinux2014_${HOSTTYPE}.whl"
elif [[ "${PYTHON_VERSION_CODE}" == "py39" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp39-cp39-manylinux2014_${HOSTTYPE}.whl"
elif [[ "${PYTHON_VERSION_CODE}" == "py310" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp310-cp310-manylinux2014_${HOSTTYPE}.whl"
elif [[ "${PYTHON_VERSION_CODE}" == "py311" ]]; then
    WHEEL_FILE="ray-${RAY_VERSION}-cp311-cp311-manylinux2014_${HOSTTYPE}.whl"
else
    echo "Unknown python version code: ${PYTHON_VERSION_CODE}" >/dev/stderr
    exit 1
fi

if [[ "${PUSH_COMMIT_TAGS:-}" == "" ]]; then
    # During branch cut, do not modify ray version in this script
    if [[ "${BUILDKITE_BRANCH:-}" == "master" || "${RAY_VERSION}" != "3.0.0dev" ]]; then
        PUSH_COMMIT_TAGS="true"
    else
        PUSH_COMMIT_TAGS="false"
    fi
fi

mkdir -p .whl

FULL_COMMIT="$(git rev-parse HEAD)"
SHORT_COMMIT="${FULL_COMMIT:0:6}"  # Use 6 chars to be consistent with Ray upstream
# During branch cut, do not modify ray version in this script
if [[ "${RAY_VERSION:-}" != "3.0.0dev" ]]; then
    SHORT_COMMIT="${RAY_VERSION}.${SHORT_COMMIT}"
fi

echo "--- Fetch wheel and base image"

aws s3 cp "${S3_TEMP}/${WHEEL_FILE}" ".whl/${WHEEL_FILE}"

BASE_IMAGE="${CI_TMP_REPO}:${IMAGE_PREFIX}-base-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}${IMAGE_SUFFIX}"

docker pull "${BASE_IMAGE}"
# This retagging is required because the Dockerfile hardcodes the base image.
docker tag "${BASE_IMAGE}" "rayproject/ray-deps:nightly-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}"

DEST_IMAGE="${RUNTIME_REPO}:${IMAGE_PREFIX}-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}${IMAGE_SUFFIX}"

echo "--- Build ${DEST_IMAGE}"

CPU_TMP="$(mktemp -d)"

mkdir -p "${CPU_TMP}/.whl"

cp ".whl/${WHEEL_FILE}" "${CPU_TMP}/.whl/${WHEEL_FILE}"
cp docker/ray/Dockerfile "${CPU_TMP}/Dockerfile"
cp python/requirements_compiled.txt "${CPU_TMP}/."
cp python/requirements_compiled_py37.txt "${CPU_TMP}/."

if [[ "${PYTHON_VERSION_CODE}" == "py37" ]]; then
    CONSTRAINTS_FILE="requirements_compiled_py37.txt"
else
    CONSTRAINTS_FILE="requirements_compiled.txt"
fi

(
    cd "${CPU_TMP}"
    tar --mtime="UTC 2020-01-01" -c -f - . \
        | docker build --progress=plain \
            --build-arg BASE_IMAGE="-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}" \
            --build-arg WHEEL_PATH=".whl/${WHEEL_FILE}" \
            --build-arg CONSTRAINTS_FILE="${CONSTRAINTS_FILE}" \
            -t "${DEST_IMAGE}" -f Dockerfile -
)

docker push "${DEST_IMAGE}"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    DEST_COMMIT_IMAGE="${RUNTIME_REPO}:${SHORT_COMMIT}-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}${IMAGE_SUFFIX}"
    docker tag "${DEST_IMAGE}" "${DEST_COMMIT_IMAGE}"
    docker push "${DEST_COMMIT_IMAGE}"
    if [[ "${IMAGE_TYPE}" == "${ML_CUDA_VERSION}" ]]; then
        DEST_ALIAS_IMAGE="${RUNTIME_REPO}:${SHORT_COMMIT}-${PYTHON_VERSION_CODE}-gpu${IMAGE_SUFFIX}"
        docker tag "${DEST_IMAGE}" "${DEST_ALIAS_IMAGE}"
        docker push "${DEST_ALIAS_IMAGE}"
    fi
fi

# This retagging is required because the Dockerfile hardcodes the base image.
docker tag "${DEST_IMAGE}" "rayproject/ray:nightly-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}${IMAGE_SUFFIX}"

buildkite-agent annotate --style=info \
    --context="${PYTHON_VERSION_CODE}-images" \
    --append "Image: ${DEST_IMAGE}<br/>"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    buildkite-agent annotate --style=info \
        --context="${PYTHON_VERSION_CODE}-images" \
        --append "Image (commit tagged): ${DEST_COMMIT_IMAGE}<br/>"
fi

if [[ "${PYTHON_VERSION_CODE}" == "py37" || "${PYTHON_VERSION_CODE}" == "py311" || "${HOSTTYPE}" == "aarch64" ]]; then
    echo "Skipping ML image for ${PYTHON_VERSION_CODE}" and host ${HOSTTYPE}
    exit 0
fi

# Build and push the ML image.
DEST_ML_IMAGE="${RUNTIME_ML_REPO}:${IMAGE_PREFIX}-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}${IMAGE_SUFFIX}"
echo "--- Build ${DEST_ML_IMAGE}"

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
            --build-arg BASE_IMAGE="-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}" \
            -t "${DEST_ML_IMAGE}" -f Dockerfile -
)

docker push "${DEST_ML_IMAGE}"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    DEST_COMMIT_ML_IMAGE="${RUNTIME_ML_REPO}:${SHORT_COMMIT}-${PYTHON_VERSION_CODE}-${IMAGE_TYPE}${IMAGE_SUFFIX}"
    docker tag "${DEST_ML_IMAGE}" "${DEST_COMMIT_ML_IMAGE}"
    docker push "${DEST_COMMIT_ML_IMAGE}"
    if [[ "${IMAGE_TYPE}" == "${ML_CUDA_VERSION}" ]]; then
        DEST_ALIAS_IMAGE="${RUNTIME_ML_REPO}:${SHORT_COMMIT}-${PYTHON_VERSION_CODE}-gpu${IMAGE_SUFFIX}"
        docker tag "${DEST_ML_IMAGE}" "${DEST_ALIAS_IMAGE}"
        docker push "${DEST_ALIAS_IMAGE}"
    fi
fi

buildkite-agent annotate --style=info \
    --context="${PYTHON_VERSION_CODE}-images" \
    --append "ML image: ${DEST_ML_IMAGE}<br/>"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    buildkite-agent annotate --style=info \
        --context="${PYTHON_VERSION_CODE}-images" \
        --append "ML image (commit tagged): ${DEST_COMMIT_ML_IMAGE}<br/>"
fi

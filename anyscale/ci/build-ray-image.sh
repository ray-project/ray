#!/bin/bash

set -euo pipefail

PY_VERSION="${1:-3.8}"
IMG_TYPE="${2:-cpu}"
BASE_TYPE="${3:-ray}"

source anyscale/ci/setup-env.sh

IMAGE_PREFIX="${RAYCI_BUILD_ID}"

UPSTREAM_COMMIT="$(cat .UPSTREAM)"
if [[ "${UPSTREAM_COMMIT}" == "" ]]; then
    echo "No upstream commit found" >/dev/stderr
    exit 1
fi

UPSTREAM_BRANCH="master"
if [[ "${RAY_VERSION}" != "3.0.0.dev0" ]]; then
    UPSTREAM_BRANCH="releases/${RAY_VERSION}"
fi

# TODO(aslonnie): add some graceful wait for the wheel from upstream to be built.
# Normally at thist point, the wheel should have already been built, but there
# is no hard guarantee.
OSS_WHEEL_URL_PREFIX="https://ray-wheels.s3.us-west-2.amazonaws.com/${UPSTREAM_BRANCH}/${UPSTREAM_COMMIT}/"

if [[ "${BASE_TYPE}" == "ray" ]]; then
    RUNTIME_REPO="830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/runtime"
elif [[ "${BASE_TYPE}" == "ray-ml" ]]; then
    RUNTIME_REPO="830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/runtime-ml"
else
    echo "Unknown base type: ${BASE_TYPE}" >/dev/stderr
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
    PY_VERSION_CODE="py38"
    WHEEL_PYTHON_CODE="cp38-cp38"
elif [[ "${PY_VERSION}" == "3.9" ]]; then
    PY_VERSION_CODE="py39"
    WHEEL_PYTHON_CODE="cp39-cp39"
elif [[ "${PY_VERSION}" == "3.10" ]]; then
    PY_VERSION_CODE="py310"
    WHEEL_PYTHON_CODE="cp310-cp310"
elif [[ "${PY_VERSION}" == "3.11" ]]; then
    PY_VERSION_CODE="py311"
    WHEEL_PYTHON_CODE="cp311-cp311"
else
    echo "Unknown python version code: ${PY_VERSION}" >/dev/stderr
    exit 1
fi

WHEEL_FILE="ray-${RAY_VERSION}-${WHEEL_PYTHON_CODE}-manylinux2014_${HOSTTYPE}.whl"
CPP_WHEEL_FILE="ray_cpp-${RAY_VERSION}-${WHEEL_PYTHON_CODE}-manylinux2014_${HOSTTYPE}.whl"

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
    buildkite-agent annotate --style=info \
        --context="${PY_VERSION_CODE}-images" --append "${DEST_IMG}<br/>"
}

function docker_push {
    local IMG="$1"
    docker push "${IMG}"
    buildkite-agent annotate --style=info \
        --context="${PY_VERSION_CODE}-images" --append "${IMG}<br/>"
}

if [[ "${PUSH_COMMIT_TAGS:-}" == "" ]]; then
    if [[ "${BUILDKITE_BRANCH:-}" =~ ^(master|releases/) ]]; then
        PUSH_COMMIT_TAGS="true"
    else
        PUSH_COMMIT_TAGS="false"
    fi
fi

BUILD_TMP="$(mktemp -d)"

mkdir -p "${BUILD_TMP}/oss-whl"
mkdir -p "${BUILD_TMP}/runtime-whl"

FULL_COMMIT="$(git rev-parse HEAD)"


####
echo "--- Fetch wheel and base image"
####

echo "OSS wheel: ${OSS_WHEEL_URL_PREFIX}${WHEEL_FILE}"
curl -sfL "${OSS_WHEEL_URL_PREFIX}${WHEEL_FILE}" -o "${BUILD_TMP}/oss-whl/${WHEEL_FILE}"
curl -sfL "${OSS_WHEEL_URL_PREFIX}${CPP_WHEEL_FILE}" -o "${BUILD_TMP}/oss-whl/${CPP_WHEEL_FILE}"

aws s3 cp "${S3_TEMP}/${WHEEL_FILE}" "${BUILD_TMP}/runtime-whl/${WHEEL_FILE}"

readonly ANYSCALE_DATAPLANE_LAYER="s3://runtime-release-test-artifacts/dataplane/20230831/dataplane.tar.gz"
aws s3 cp "${ANYSCALE_DATAPLANE_LAYER}" "${BUILD_TMP}/dataplane.tar.gz"
DATAPLANE_TGZ_GOT="$(sha256sum "${BUILD_TMP}/dataplane.tar.gz" | cut -d' ' -f1)"
DATAPLANE_TGZ_WANT="3658592a3b30a93b2e940269f99296e822446eed7ef36049d707b435ff868638"
if [[ "${DATAPLANE_TGZ_GOT}" != "${DATAPLANE_TGZ_WANT}" ]]; then
    echo "Dataplane tarball sha256 digest:" \
        "got ${DATAPLANE_TGZ_GOT}, want ${DATAPLANE_TGZ_WANT}" >/dev/stderr
    exit 1
fi

BASE_IMG="${RAYCI_WORK_REPO}:${IMAGE_PREFIX}-${BASE_TYPE}-py${PY_VERSION}-${IMG_TYPE}-base"

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin "${RUNTIME_ECR}"

docker pull "${BASE_IMG}"

if [[ "${BUILDKITE:-}" == "true" ]]; then
    rm -rf /artifact-mount/sitepkg
    mkdir -p /artifact-mount/sitepkg/ray-oss
    mkdir -p /artifact-mount/sitepkg/ray-opt
fi

# Everything is prepared, starts building now.

export DOCKER_BUILDKIT=1

BUILD_TAG="${IMAGE_PREFIX}-${PY_VERSION_CODE}-${IMG_TYPE_CODE}${IMG_SUFFIX}"
RAY_IMG="${RUNTIME_REPO}:${BUILD_TAG}"
ANYSCALE_IMG="${RUNTIME_REPO}:${BUILD_TAG}-as"
SITEPKG_TGZ="${BASE_TYPE}-${PY_VERSION_CODE}-${IMG_TYPE_CODE}${IMG_SUFFIX}.tar.gz"

####
echo "--- Step 1: Build OSS site package tarball"
####

CONTEXT_TMP="$(mktemp -d)"

mkdir -p "${CONTEXT_TMP}/.whl"
cp "${BUILD_TMP}/oss-whl/${WHEEL_FILE}" "${CONTEXT_TMP}/.whl/${WHEEL_FILE}"
cp "${BUILD_TMP}/oss-whl/${CPP_WHEEL_FILE}" "${CONTEXT_TMP}/.whl/${CPP_WHEEL_FILE}"
cp python/requirements_compiled.txt "${CONTEXT_TMP}/."
cp anyscale/docker/Dockerfile.sitepkg "${CONTEXT_TMP}/Dockerfile"

(
    cd "${CONTEXT_TMP}"
    tar --mtime="UTC 2020-01-01" --sort=name -c -f - . \
        | docker build --progress=plain \
            --build-arg FULL_BASE_IMAGE="${BASE_IMG}" \
            --build-arg RAY_VERSION="${RAY_VERSION}" \
            --build-arg WHEEL_PATH=".whl/${WHEEL_FILE}" \
            --build-arg RAY_MOD_DATE="2020-01-01" \
            --output="${BUILD_TMP}" --target=final -f Dockerfile -
)

mv "${BUILD_TMP}/ray.tgz" "${BUILD_TMP}/ray-oss.tgz"

aws s3 cp "${BUILD_TMP}/ray-oss.tgz" "${S3_TEMP}/ray-oss/${PY_VERSION_CODE}/${SITEPKG_TGZ}"
if [[ "${BUILDKITE:-}" == "true" ]]; then
    cp "${BUILD_TMP}/ray-oss.tgz" "/artifact-mount/sitepkg/ray-oss/${SITEPKG_TGZ}"
fi


####
echo "--- Step 2: Build Runtime site package tarball"
####

# Only need to overwrite the wheel
cp "${BUILD_TMP}/runtime-whl/${WHEEL_FILE}" "${CONTEXT_TMP}/.whl/${WHEEL_FILE}"
rm "${CONTEXT_TMP}/.whl/${CPP_WHEEL_FILE}" # And removes the ray-cpp wheel.

# Runtime uses a later date, this will force pyc file recompile after
# extraction.
(
    cd "${CONTEXT_TMP}"
    tar --mtime="UTC 2023-01-01" --sort=name -c -f - . \
        | docker build --progress=plain \
            --build-arg FULL_BASE_IMAGE="${BASE_IMG}" \
            --build-arg RAY_VERSION="${RAY_VERSION}" \
            --build-arg WHEEL_PATH=".whl/${WHEEL_FILE}" \
            --build-arg RAY_MOD_DATE="2023-01-01" \
            --output="${BUILD_TMP}" --target=final -f Dockerfile -
)

mv "${BUILD_TMP}/ray.tgz" "${BUILD_TMP}/ray-opt.tgz" 

aws s3 cp "${BUILD_TMP}/ray-opt.tgz" "${S3_TEMP}/ray-opt/${PY_VERSION_CODE}/${SITEPKG_TGZ}"
if [[ "${BUILDKITE:-}" == "true" ]]; then
    cp "${BUILD_TMP}/ray-opt.tgz" "/artifact-mount/sitepkg/ray-opt/${SITEPKG_TGZ}"
fi

# Cleanup sitepkg build context.
rm -rf "${CONTEXT_TMP}"


####
echo "--- Step 3: Build ${RAY_IMG}"
####

CONTEXT_TMP="$(mktemp -d)"

mkdir -p "${CONTEXT_TMP}/.whl"

cp "${BUILD_TMP}/runtime-whl/${WHEEL_FILE}" "${CONTEXT_TMP}/.whl/${WHEEL_FILE}"
cp anyscale/docker/Dockerfile.ray "${CONTEXT_TMP}/Dockerfile"
cp anyscale/docker/runtime-requirements.txt "${CONTEXT_TMP}/runtime-requirements.txt"
cp python/requirements_compiled.txt "${CONTEXT_TMP}/."
cp anyscale/docker/NOTICE "${CONTEXT_TMP}/."
cp LICENSE.runtime "${CONTEXT_TMP}/LICENSE"

# TODO(aslonnie): replace this with ray-oss.tgz when Anyscale has support
# to inject ray-opt.tgz into the image.
cp "${BUILD_TMP}/ray-opt.tgz" "${CONTEXT_TMP}/ray.tgz"

(
    cd "${CONTEXT_TMP}"
    tar --mtime="UTC 2023-10-01" --sort=name -c -f - . \
        | docker build --progress=plain \
            --build-arg FULL_BASE_IMAGE="${BASE_IMG}" \
            --build-arg WHEEL_PATH=".whl/${WHEEL_FILE}" \
            --build-arg RAY_VERSION="${RAY_VERSION}" \
            --build-arg RAY_SITE_PKG_TGZ=ray.tgz \
            -t "${RAY_IMG}" -f Dockerfile -
)

rm -rf "${CONTEXT_TMP}"

echo "--- Build ${ANYSCALE_IMG}"
docker build --progress=plain \
    --build-arg BASE_IMAGE="${RAY_IMG}" \
    -t "${ANYSCALE_IMG}" -f Dockerfile - < "${BUILD_TMP}/dataplane.tar.gz"


####
echo "--- Pushing images"
####

docker_push "${RAY_IMG}"
docker_push "${ANYSCALE_IMG}"

if [[ "${PUSH_COMMIT_TAGS}" == "true" ]]; then
    SHORT_COMMIT="${FULL_COMMIT:0:6}"  # Use 6 chars to be consistent with Ray upstream
    # During branch cut, do not modify ray version in this script
    if [[ ! "${RAY_VERSION:-}" =~ dev ]]; then
        SHORT_COMMIT="${RAY_VERSION}.${SHORT_COMMIT}"
    fi

    COMMIT_TAG="${SHORT_COMMIT}-${PY_VERSION_CODE}-${IMG_TYPE_CODE}${IMG_SUFFIX}"
    docker_push_as "${RAY_IMG}" "${RUNTIME_REPO}:${COMMIT_TAG}"
    docker_push_as "${ANYSCALE_IMG}" "${RUNTIME_REPO}:${COMMIT_TAG}-as"
    
    if [[ "${IMG_TYPE_CODE}" == "${ML_CUDA_VERSION}" ]]; then
        COMMIT_GPU_TAG="${SHORT_COMMIT}-${PY_VERSION_CODE}-gpu${IMG_SUFFIX}"
        docker_push_as "${RAY_IMG}" "${RUNTIME_REPO}:${COMMIT_GPU_TAG}"
        docker_push_as "${ANYSCALE_IMG}" "${RUNTIME_REPO}:${COMMIT_GPU_TAG}-as"
    fi
fi

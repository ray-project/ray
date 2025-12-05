#!/bin/bash
# Nightly for building and uploading the manylinux image

set -euo pipefail

trap '[ $? -eq 0 ] || printError "Command failed ${BASH_COMMAND}"' EXIT

REPO_ROOT_DIR=$(git rev-parse --show-toplevel)
# shellcheck source=ci/base_images/utils/common.sh
source "${REPO_ROOT_DIR}/ci/base_images/utils/common.sh"
# shellcheck source=ci/base_images/utils/lib-docker.sh
source "${REPO_ROOT_DIR}/ci/base_images/utils/lib-docker.sh"

printInfo "Starting manylinux build and upload process"

ARCH=$(normalize_arch)
printInfo "Using architecture: $ARCH"

REGISTRY=${REGISTRY:-"cr.ray.io/rayproject"}
DOCKERFILE="${REPO_ROOT_DIR}/ci/docker/manylinux.Dockerfile"
IMAGE_NAME="${REGISTRY}/manylinux-${ARCH}"
COMMIT_HASH=$(git rev-parse HEAD)
IMAGE_TAG="${IMAGE_NAME}:${COMMIT_HASH}"
BUILD_CONTEXT="${REPO_ROOT_DIR}"

BUILD_ARGS=$(cat <<EOF
BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL:-}
RAYCI_DISABLE_JAVA=${RAYCI_DISABLE_JAVA:-false}
HOSTTYPE=${ARCH}
EOF
)

printBuildkiteHeader "Building manylinux image"
build_image "$DOCKERFILE" "$IMAGE_TAG" "$REGISTRY" "$BUILD_CONTEXT" "$BUILD_ARGS"

printBuildkiteHeader "Uploading manylinux image"
upload_image "$IMAGE_TAG" "$REGISTRY"

printBuildkiteHeader "Done"

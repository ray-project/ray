#!/bin/bash

# Push manylinux Docker images sourced from Wanda build to Docker Hub
# Usage:
#   ./ci/base_images/nightly/push-manylinux.sh [--upload]
# Options:
#   --upload           Upload the image to the registry
#   -h, --help         Show this help message and exit

set -euo pipefail

trap '[ $? -eq 0 ] || printError "Command failed: ${BASH_COMMAND}"' EXIT

REPO_ROOT_DIR=$(git rev-parse --show-toplevel)
# shellcheck source=ci/base_images/utils/common.sh
source "${REPO_ROOT_DIR}/ci/base_images/utils/common.sh"

: "${RAYCI_WORK_REPO:?Error: RAYCI_WORK_REPO is not set}"
: "${RAYCI_BUILD_ID:?Error: RAYCI_BUILD_ID is not set}"
: "${JDK_SUFFIX:?Error: JDK_SUFFIX is not set}"
UPLOAD=${UPLOAD:-"false"}

COMMIT_HASH=$(git rev-parse HEAD)
ARCH=$(normalize_arch)
WANDA_IMAGE_NAME="manylinux-nightly${JDK_SUFFIX}"
WANDA_TAG="${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${WANDA_IMAGE_NAME}"
REMOTE_TAG="rayproject/manylinux2024_${ARCH}${JDK_SUFFIX}:${COMMIT_HASH}"

printInfo "Source tag (Wanda): ${WANDA_TAG}"
printInfo "Target tag (Docker Hub): ${REMOTE_TAG}"

if [[ "$UPLOAD" == "true" ]]; then
    printHeader "Checking if image already exists on Docker Hub"
    if docker manifest inspect "$REMOTE_TAG" &>/dev/null; then
        printInfo "Image already exists: $REMOTE_TAG"
        printInfo "Nothing to do, exiting successfully"
        exit 0
    fi
    printInfo "Image does not exist, proceeding with push"
fi

printHeader "Pulling image from Wanda cache"
printInfo "Pulling: ${WANDA_TAG}"
docker pull "$WANDA_TAG"

if [[ "$UPLOAD" == "true" ]]; then
    printHeader "Authenticating with Docker Hub"
    bazel run //.buildkite:copy_files -- --destination docker_login
fi

printHeader "Publishing image to Docker Hub"

if [[ "$UPLOAD" == "true" ]]; then
    printInfo "Tagging: ${WANDA_TAG} -> ${REMOTE_TAG}"
    docker tag "$WANDA_TAG" "$REMOTE_TAG"
    
    printInfo "Pushing to Docker Hub..."
    docker push "$REMOTE_TAG"
    
    printInfo "Successfully published: $REMOTE_TAG"
else
    printInfo "DRY RUN MODE - No changes will be made"
    printInfo "  Would tag: ${WANDA_TAG}"
    printInfo "           → ${REMOTE_TAG}"
    printInfo "  Would push to Docker Hub"
    printInfo ""
    printInfo "To actually push, run with: --upload"
fi

printHeader "Done"

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

UPLOAD=${UPLOAD:-"false"}
COMMIT_HASH=$(git rev-parse HEAD)
ARCH=$(normalize_arch)

printHeader "Pulling manylinux image from Wanda cache"
wanda_image_name="manylinux-nightly${JDK_SUFFIX}"
# E.g. Tag output as 029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:779d7467-manylinux
wanda_tag="${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${wanda_image_name}"
printInfo "Pulling manylinux image (${wanda_image_name}) from Wanda cache: ${wanda_tag}"
docker pull "$wanda_tag"

printHeader "Handling Docker login"
if [[ "$UPLOAD" == "true" ]]; then
    printInfo "Logging into Docker registry"
    bazel run //.buildkite:copy_files -- --destination docker_login
else
    printInfo "Skipping docker login (upload is disabled)"
fi

printHeader "Tagging image"
REMOTE_REGISTRY_PREFIX="rayproject/manylinux2024_${ARCH}"
remote_tag="${REMOTE_REGISTRY_PREFIX}${jdk_suffix}:${COMMIT_HASH}"
printInfo "Local tag: $local_tag"
printInfo "Remote tag: $remote_tag"

printHeader "Pushing image to Docker Hub"
if [[ "$UPLOAD" == "true" ]]; then
    printInfo "Checking if remote tag already exists on Docker Hub"
    
    if docker manifest inspect "$remote_tag" &>/dev/null; then
        printInfo "Remote tag already exists on Docker Hub, skipping push"
        exit 0
    fi

    printInfo "Tagging image for Docker Hub"
    docker tag "$local_tag" "$remote_tag"
    
    printInfo "Pushing image to Docker Hub: $remote_tag"
    docker push "$remote_tag"
    printInfo "Successfully uploaded: $remote_tag"
else
    printInfo "Skipping upload (use --upload flag to enable)"
    printInfo "Would tag: $local_tag -> $remote_tag"
fi

printHeader "Success"

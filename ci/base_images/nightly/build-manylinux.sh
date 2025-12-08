#!/bin/bash

# Build and optionally upload manylinux Docker images (with and without JDK)
# Usage:
#   ./ci/base_images/nightly/build-manylinux.sh [--upload]
# Options:
#   --upload           Upload the images to the registry
#   -h, --help         Show this help message and exit

set -euo pipefail

trap '[ $? -eq 0 ] || printError "Command failed: ${BASH_COMMAND}"' EXIT

REPO_ROOT_DIR=$(git rev-parse --show-toplevel)
# shellcheck source=ci/base_images/utils/common.sh
source "${REPO_ROOT_DIR}/ci/base_images/utils/common.sh"

# Parsing CLI args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --upload)
            UPLOAD="true"
            ;;
        -h|--help)
            echo "Usage: $0 [--upload]"
            echo "Options:"
            echo "  --upload           Upload the images to the registry"
            echo "  -h, --help         Show this help message and exit"
            exit 0
            ;;
        *)
            echo "Invalid argument: $1"
            echo "$usage"
            exit 1
            ;;
    esac
    shift
done

# Config
UPLOAD=${UPLOAD:-"false"}
WANDA_YAML_PATH="${REPO_ROOT_DIR}/ci/docker/manylinux.wanda.yaml"
COMMIT_HASH=$(git rev-parse HEAD)
LOCAL_REGISTRY="localhost:5000/rayci-work"
ARCH=$(normalize_arch)
REMOTE_REGISTRY_PREFIX="rayproject/manylinux2024_${ARCH}"

# NOTE(andrew-anyscale): We can remove this if we can safely source the wanda binary from elsewhere.
printHeader "Installing wanda binary"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
WANDA_BIN_PATH="$TMP_DIR/wanda"
install_wanda "$WANDA_BIN_PATH"

printHeader "Handling Docker login"
if [[ "$UPLOAD" == "true" ]]; then
    printInfo "Logging into Docker registry"
    bazel run //.buildkite:copy_files -- --destination docker_login
else
    printInfo "Skipping docker login (upload is disabled)"
fi

# Build both variants (with and without JDK)
for with_jdk in true false; do
    if [[ "$with_jdk" == "true" ]]; then
        jdk_label="with JDK"
        jdk_suffix="_jdk"
        export RAYCI_DISABLE_JAVA="false"
    else
        jdk_label="_no_ JDK"
        jdk_suffix=""
        export RAYCI_DISABLE_JAVA="true"
    fi
    
    printHeader "Building manylinux image ${jdk_label}"
    
    build_id=$(openssl rand -hex 4)
    local_tag="${LOCAL_REGISTRY}:${build_id}-manylinux"
    remote_tag="${REMOTE_REGISTRY_PREFIX}${jdk_suffix}:${COMMIT_HASH}"
    
    # Clean up any existing local image
    if docker image inspect "$local_tag" &>/dev/null; then
        printWarn "Removing existing local image: $local_tag"
        docker rmi "$local_tag"
    fi
    
    # Build image with wanda
    printInfo "Building: $local_tag"
    "$WANDA_BIN_PATH" -build_id="${build_id}" -work_dir="${REPO_ROOT_DIR}" "$WANDA_YAML_PATH"
    
    # Verify image was built with expected tag
    if ! docker image inspect "$local_tag" &>/dev/null; then
        printError "Build failed - image not found: $local_tag"
        exit 1
    fi
    
    if [[ "$UPLOAD" == "true" ]]; then
        printInfo "Tagging and pushing: $remote_tag"

        if docker manifest inspect "$remote_tag" &>/dev/null; then
            printInfo "Remote tag already exists on Docker Hub, skipping push"
            continue
        fi

        docker tag "$local_tag" "$remote_tag"
        docker push "$remote_tag"
        printInfo "Successfully uploaded: $remote_tag"
    else
        printInfo "Skipping upload: $local_tag -> $remote_tag"
    fi
done

printHeader "Success"

if [[ "$UPLOAD" == "true" ]]; then
    printInfo "Successfully built and uploaded manylinux images"
else
    printInfo "Successfully built manylinux images, but upload is disabled"
fi

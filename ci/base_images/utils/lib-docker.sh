#!/bin/bash
# Generic Docker operations library

REPO_ROOT_DIR=$(git rev-parse --show-toplevel)
# shellcheck source=ci/base_images/utils/common.sh
source "${REPO_ROOT_DIR}/ci/base_images/utils/common.sh"

# Login to Docker registry
login_to_registry() {
    local registry="$1"
    
    # Docker handles caching internally, safe to call multiple times
    printInfo "Logging in to $registry"
    docker login "$registry"
}

# Build Docker image
# Args:
#   $1: dockerfile path
#   $2: image tag
#   $3: registry
#   $4: build context
#   $5: build args (optional, newline-separated string)
build_image() {
    local dockerfile="$1"
    local image_tag="$2"
    local registry="$3"
    local build_context="$4"
    local build_args="${5:-}"
    
    validate_file_exists "$dockerfile"
    login_to_registry "$registry"
    
    printInfo "Building image: ${image_tag}"
    
    local build_cmd=(docker build -t "$image_tag" -f "$dockerfile")
    
    if [[ -n "$build_args" ]]; then
        while IFS= read -r arg; do
            [[ -n "$arg" ]] && build_cmd+=(--build-arg "$arg")
        done <<< "$build_args"
    fi
    
    build_cmd+=("$build_context")

    # Log the command for debugging
    printInfo "Running: ${build_cmd[*]}"
    
    "${build_cmd[@]}"
    
    printInfo "Build complete: ${image_tag}"
}

# Upload Docker image to registry
# Args:
#   $1: image tag
#   $2: registry
upload_image() {
    local image_tag="$1"
    local registry="$2"
    
    login_to_registry "$registry"
    
    printInfo "Pushing image: ${image_tag}"
    
    docker push "$image_tag"
    
    printInfo "Upload complete: ${image_tag}"
}


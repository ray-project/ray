#!/bin/bash
#
# [WIP] Local development script for building Ray docker images using wanda.
#
# This script is for local iteration and testing of wanda-based image builds.
# It requires the wanda binary to be installed (see WANDA_BIN below).
#
# Note: This script will build the ray wheel first if needed (images depend on wheels).
#
# Usage (from repo root):
#   ci/build/build-ray-image-local.sh                      # Build CPU image for Python 3.10
#   ci/build/build-ray-image-local.sh 3.11                 # Build CPU image for Python 3.11
#   ci/build/build-ray-image-local.sh 3.10 cpu             # Build CPU image (default)
#   ci/build/build-ray-image-local.sh 3.10 gpu             # Build GPU image (CUDA 12.1.1)
#   ci/build/build-ray-image-local.sh 3.10 cuda 11.8.0-cudnn8  # Build specific CUDA version
#
set -euo pipefail

# Default GPU platform (matches GPU_PLATFORM in ci/ray_ci/docker_container.py)
GPU_PLATFORM="12.1.1-cudnn8"

header() {
    echo -e "\n\033[34;1m===> $1\033[0m"
}

usage() {
    echo "Usage: $0 [PYTHON_VERSION] [IMAGE_TYPE] [CUDA_VERSION]"
    echo ""
    echo "Image types:"
    echo "  cpu (default)  - Build ray CPU docker image"
    echo "  gpu            - Build ray GPU docker image (CUDA ${GPU_PLATFORM})"
    echo "  cuda           - Build ray CUDA docker image (requires CUDA_VERSION as next arg)"
    echo ""
    echo "Examples:"
    echo "  $0 3.10                        # Build CPU image"
    echo "  $0 3.10 cpu                    # Build CPU image"
    echo "  $0 3.10 gpu                    # Build GPU image (default CUDA)"
    echo "  $0 3.10 cuda 11.8.0-cudnn8     # Build specific CUDA version"
    exit 1
}

# Handle help early
if [[ "${1:-}" == "help" || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
fi

# Parse arguments
PYTHON_VERSION="${1:-3.10}"
IMAGE_TYPE="${2:-cpu}"
CUDA_VERSION="${3:-}"
WANDA_BIN="${WANDA_BIN:-/home/ubuntu/rayci/bin/wanda}"

# Handle gpu as alias for cuda with default version
if [[ "$IMAGE_TYPE" == "gpu" ]]; then
    IMAGE_TYPE="cuda"
    CUDA_VERSION="$GPU_PLATFORM"
fi

# Validate cuda requires version
if [[ "$IMAGE_TYPE" == "cuda" && -z "$CUDA_VERSION" ]]; then
    echo "Error: cuda image type requires CUDA_VERSION argument"
    usage
fi

# Configuration
export PYTHON_VERSION
export ARCH_SUFFIX=""
export HOSTTYPE="x86_64"
export MANYLINUX_VERSION="251216.3835fc5"

echo "Building Ray docker image for Python ${PYTHON_VERSION}..."
echo "    Image type: ${IMAGE_TYPE}"
[[ "$IMAGE_TYPE" == "cuda" ]] && echo "    CUDA version: ${CUDA_VERSION}"

# Build common wheel dependencies (these are cached by wanda)
build_wheel_deps() {
    header "Building ray-core..."
    $WANDA_BIN ci/docker/ray-core.wanda.yaml

    header "Building ray-dashboard..."
    $WANDA_BIN ci/docker/ray-dashboard.wanda.yaml

    header "Building ray-java..."
    $WANDA_BIN ci/docker/ray-java.wanda.yaml

    # Tag images locally so the @ references in wanda.yaml files work
    header "Tagging images for local wanda access..."
    docker tag "cr.ray.io/rayproject/ray-core-py${PYTHON_VERSION}:latest" "ray-core-py${PYTHON_VERSION}:latest"
    docker tag "cr.ray.io/rayproject/ray-java-build:latest" "ray-java-build:latest"
    docker tag "cr.ray.io/rayproject/ray-dashboard:latest" "ray-dashboard:latest"
}

# Build ray wheel (required for images)
build_ray_wheel() {
    build_wheel_deps

    header "Building ray-wheel..."
    $WANDA_BIN ci/docker/ray-wheel.wanda.yaml

    # Tag for local image reference
    docker tag "cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}:latest" "ray-wheel-py${PYTHON_VERSION}:latest"
}

# Build CPU base image
build_cpu_base() {
    export REQUIREMENTS_FILE="ray_base_deps_py${PYTHON_VERSION}.lock"

    header "Building CPU base image..."
    $WANDA_BIN docker/base-deps/cpu.wanda.yaml

    # Tag for local access
    local BASE_IMAGE="ray-py${PYTHON_VERSION}-cpu-base"
    docker tag "cr.ray.io/rayproject/${BASE_IMAGE}:latest" "${BASE_IMAGE}:latest"
}

# Build CUDA base image
build_cuda_base() {
    local cuda_version="$1"
    export CUDA_VERSION="${cuda_version}"
    export REQUIREMENTS_FILE="ray_base_deps_py${PYTHON_VERSION}.lock"

    header "Building CUDA ${cuda_version} base image..."
    $WANDA_BIN docker/base-deps/cuda.wanda.yaml

    # Tag for local access
    local BASE_IMAGE="ray-py${PYTHON_VERSION}-cu${cuda_version}-base"
    docker tag "cr.ray.io/rayproject/${BASE_IMAGE}:latest" "${BASE_IMAGE}:latest"
}

# Build CPU docker image
build_image_cpu() {
    # Ensure ray wheel is built first
    build_ray_wheel

    # Build base image
    build_cpu_base

    header "Building ray CPU image..."
    $WANDA_BIN ci/docker/ray-image-cpu.wanda.yaml

    header "Ray CPU image build complete!"
    echo "    Image: cr.ray.io/rayproject/ray:nightly-py${PYTHON_VERSION}-cpu"
}

# Build CUDA docker image
build_image_cuda() {
    local cuda_version="$1"

    # Ensure ray wheel is built first
    build_ray_wheel

    # Build base image
    build_cuda_base "$cuda_version"

    export CUDA_VERSION="${cuda_version}"
    header "Building ray CUDA ${cuda_version} image..."
    $WANDA_BIN ci/docker/ray-image-cuda.wanda.yaml

    header "Ray CUDA image build complete!"
    echo "    Image: cr.ray.io/rayproject/ray:nightly-py${PYTHON_VERSION}-cu${cuda_version}"
}

# Main build logic
case "$IMAGE_TYPE" in
    cpu)
        build_image_cpu
        ;;
    cuda)
        build_image_cuda "$CUDA_VERSION"
        ;;
    *)
        echo "Unknown image type: $IMAGE_TYPE"
        usage
        ;;
esac

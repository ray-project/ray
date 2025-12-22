#!/bin/bash
#
# [WIP] Local development script for building Ray docker images using wanda.
#
# This script is for local iteration and testing of wanda-based image builds.
# It requires the wanda binary to be installed (see WANDA_BIN below).
#
# ============================================================================
# IMAGE BUILD HIERARCHY
# ============================================================================
#
# This script builds publishable Ray docker images (ray:nightly-py3.10-cpu, etc).
# Unlike test containers, these require a wheel build to create distributable images.
#
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                     PRE-BUILT ARTIFACTS                                 │
#   │                                                                         │
#   │   manylinux2014                                                         │
#   │        │                                                                │
#   │        ├──────────────────┬──────────────────┐                          │
#   │        ▼                  ▼                  ▼                          │
#   │   ray-core           ray-dashboard       ray-java                       │
#   │   (C++ via bazel)    (npm build)         (Java JARs)                    │
#   │        │                  │                  │                          │
#   │        │  ray_pkg.zip     │  dashboard.tar   │  ray-java.jar            │
#   │        └──────────────────┴──────────────────┘                          │
#   │                           │                                             │
#   │                           ▼                                             │
#   │                      ray-wheel                                          │
#   │                   (pip wheel build)                                     │
#   │                           │                                             │
#   │                    ray-{ver}.whl                                        │
#   └───────────────────────────┼─────────────────────────────────────────────┘
#                               │
#                               ▼
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                      BASE IMAGES                                        │
#   │                                                                         │
#   │   CPU Path:                        CUDA Path:                           │
#   │                                                                         │
#   │   ubuntu:22.04                     nvidia/cuda:{ver}-ubuntu22.04        │
#   │        │                                  │                             │
#   │        ▼                                  ▼                             │
#   │   ray-py{ver}-cpu-base             ray-py{ver}-cu{cuda}-base            │
#   │   (docker/base-deps/cpu)           (docker/base-deps/cuda)              │
#   │        │                                  │                             │
#   └────────┼──────────────────────────────────┼─────────────────────────────┘
#            │                                  │
#            ▼                                  ▼
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                    FINAL RAY IMAGES                                     │
#   │                                                                         │
#   │   ray:nightly-py{ver}-cpu          ray:nightly-py{ver}-cu{cuda}         │
#   │   (ci/docker/ray-image-cpu)        (ci/docker/ray-image-cuda)           │
#   │                                                                         │
#   │   Contents:                                                             │
#   │   • Ray wheel installed                                                 │
#   │   • Default Ray dependencies                                            │
#   │   • Ready for: docker run rayproject/ray:nightly-py3.10-cpu             │
#   └─────────────────────────────────────────────────────────────────────────┘
#
# ============================================================================
# USAGE
# ============================================================================
#
# Examples (from repo root):
#   ci/build/build-ray-image-local.sh                      # Build CPU image for Python 3.10
#   ci/build/build-ray-image-local.sh 3.11                 # Build CPU image for Python 3.11
#   ci/build/build-ray-image-local.sh 3.10 cpu             # Build CPU image (default)
#   ci/build/build-ray-image-local.sh 3.10 gpu             # Build GPU image (CUDA 12.1.1)
#   ci/build/build-ray-image-local.sh 3.10 cuda 11.8.0-cudnn8  # Build specific CUDA version
#
set -euo pipefail

# Source shared utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/local-build-utils.sh"

# Default GPU platform (matches GPU_PLATFORM in ci/ray_ci/docker_container.py)
GPU_PLATFORM="12.1.1-cudnn8"

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

# Setup environment
setup_build_env

# Parse arguments
if [[ "${1:-}" =~ ^3\.[0-9]+$ ]]; then
    PYTHON_VERSION="$1"
    shift
else
    PYTHON_VERSION="3.10"
fi
IMAGE_TYPE="${1:-cpu}"
CUDA_VERSION="${2:-}"

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

# Check prerequisites
check_wanda_prerequisites "$WANDA_BIN"

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

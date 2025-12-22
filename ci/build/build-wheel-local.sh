#!/bin/bash
#
# [WIP] Local development script for building Ray wheels using wanda.
#
# This script is for local iteration and testing of wanda-based wheel builds.
# It requires the wanda binary to be installed (see WANDA_BIN below).
#
# ============================================================================
# WHEEL BUILD HIERARCHY
# ============================================================================
#
# This script builds distributable Ray wheel files (.whl) using pre-built
# artifacts from wanda images. The C++ compilation happens once in ray-core,
# then gets packaged into wheels.
#
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                     PRE-BUILT ARTIFACTS                                 │
#   │                                                                         │
#   │   manylinux2014 (glibc compatible base)                                 │
#   │        │                                                                │
#   │        ├──────────────────┬──────────────────┐                          │
#   │        ▼                  ▼                  ▼                          │
#   │   ray-core           ray-dashboard       ray-java                       │
#   │   (bazel build)      (npm build)         (maven build)                  │
#   │        │                  │                  │                          │
#   │   ray_pkg.zip        dashboard.tar.gz    ray-java.jar                   │
#   │   ray_py_proto.zip                                                      │
#   │        │                  │                  │                          │
#   └────────┼──────────────────┼──────────────────┼──────────────────────────┘
#            │                  │                  │
#            └──────────────────┴──────────────────┘
#                               │
#   ┌───────────────────────────┼─────────────────────────────────────────────┐
#   │                           ▼                                             │
#   │   RAY WHEEL BUILD (ray-wheel.wanda.yaml)                                │
#   │                                                                         │
#   │   1. Extract ray_pkg.zip (C++ binaries)                                 │
#   │   2. Extract dashboard.tar.gz                                           │
#   │   3. Copy ray-java.jar                                                  │
#   │   4. pip wheel . (packages everything, NO C++ compile)                  │
#   │                           │                                             │
#   │                           ▼                                             │
#   │                  ray-{version}-{platform}.whl                           │
#   │                                                                         │
#   └─────────────────────────────────────────────────────────────────────────┘
#
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │   RAY-CPP WHEEL BUILD (optional, for C++ API users)                     │
#   │                                                                         │
#   │   ray-core ──► ray-cpp-core ──► ray-cpp-wheel                           │
#   │                (gen_ray_cpp_pkg)   (pip wheel)                          │
#   │                      │                  │                               │
#   │                      ▼                  ▼                               │
#   │                 C++ headers      ray_cpp-{version}.whl                  │
#   │                 + libraries                                             │
#   └─────────────────────────────────────────────────────────────────────────┘
#
#   Output: Wheels are stored in docker images, use 'extract' to copy to .whl/
#
# ============================================================================
# USAGE
# ============================================================================
#
# Examples (from repo root):
#   ci/build/build-wheel-local.sh                      # Build ray wheel for Python 3.10
#   ci/build/build-wheel-local.sh 3.11                 # Build ray wheel for Python 3.11
#   ci/build/build-wheel-local.sh 3.10 extract         # Build and extract wheel to .whl/
#   ci/build/build-wheel-local.sh 3.10 cpp             # Build ray-cpp wheel
#   ci/build/build-wheel-local.sh 3.10 cpp extract     # Build and extract ray-cpp wheel
#   ci/build/build-wheel-local.sh 3.10 all extract     # Build both wheels and extract
#
set -euo pipefail

header() {
    echo -e "\n\033[34;1m===> $1\033[0m"
}

# Extract wheel from a docker image to .whl/ directory
extract_wheel() {
    local image_name="$1"
    local wheel_type="$2"
    header "Extracting ${wheel_type} wheel to .whl/..."
    container_id=$(docker create "${image_name}" true)
    docker cp "${container_id}:/" - | tar -xf - -C .whl --strip-components=0 '*.whl' 2>/dev/null || \
        docker cp "${container_id}:/" .whl/
    docker rm "${container_id}" > /dev/null
}

usage() {
    echo "Usage: $0 [PYTHON_VERSION] [BUILD_TYPE] [OPTIONS]"
    echo ""
    echo "Build types:"
    echo "  ray (default)  - Build ray wheel"
    echo "  cpp            - Build ray-cpp wheel"
    echo "  all            - Build both ray and ray-cpp wheels"
    echo ""
    echo "Options:"
    echo "  extract        - Extract wheel(s) to .whl/ directory"
    echo ""
    echo "Examples:"
    echo "  $0 3.10 extract           # Build and extract ray wheel"
    echo "  $0 3.10 cpp extract       # Build and extract ray-cpp wheel"
    echo "  $0 3.10 all extract       # Build and extract both wheels"
    exit 1
}

# Handle help early
if [[ "${1:-}" == "help" || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
fi

# Parse arguments
PYTHON_VERSION="${1:-3.10}"
BUILD_TYPE="${2:-ray}"
ARG3="${3:-}"
WANDA_BIN="${WANDA_BIN:-/home/ubuntu/rayci/bin/wanda}"

# Handle "extract" as second argument (backwards compatibility)
if [[ "$BUILD_TYPE" == "extract" ]]; then
    BUILD_TYPE="ray"
    EXTRACT="extract"
else
    EXTRACT="${ARG3:-}"
fi

# Configuration
export PYTHON_VERSION
export ARCH_SUFFIX=""
export HOSTTYPE="x86_64"
export MANYLINUX_VERSION="251216.3835fc5"

echo "Building Ray wheel for Python ${PYTHON_VERSION}..."
echo "    Build type: ${BUILD_TYPE}"

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

# Build ray wheel
build_ray_wheel() {
    build_wheel_deps

    header "Building ray-wheel..."
    $WANDA_BIN ci/docker/ray-wheel.wanda.yaml

    # Tag for local image reference
    docker tag "cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}:latest" "ray-wheel-py${PYTHON_VERSION}:latest"

    header "Ray wheel build complete!"
    echo "    Image: cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}:latest"
}

# Build ray-cpp wheel
build_cpp_wheel() {
    build_wheel_deps

    header "Building ray-cpp-core..."
    $WANDA_BIN ci/docker/ray-cpp-core.wanda.yaml

    # Tag cpp-core image for local access
    docker tag "cr.ray.io/rayproject/ray-cpp-core-py${PYTHON_VERSION}:latest" "ray-cpp-core-py${PYTHON_VERSION}:latest"

    header "Building ray-cpp-wheel..."
    $WANDA_BIN ci/docker/ray-cpp-wheel.wanda.yaml

    header "Ray C++ wheel build complete!"
    echo "    Image: cr.ray.io/rayproject/ray-cpp-wheel-py${PYTHON_VERSION}:latest"
}

# Main build logic
case "$BUILD_TYPE" in
    ray)
        build_ray_wheel
        ;;
    cpp)
        build_cpp_wheel
        ;;
    all)
        build_ray_wheel
        build_cpp_wheel
        ;;
    *)
        echo "Unknown build type: $BUILD_TYPE"
        usage
        ;;
esac

# Extract wheel(s) if requested
if [[ "$EXTRACT" == "extract" ]]; then
    mkdir -p .whl

    if [[ "$BUILD_TYPE" == "ray" || "$BUILD_TYPE" == "all" ]]; then
        extract_wheel "cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}:latest" "ray"
    fi

    if [[ "$BUILD_TYPE" == "cpp" || "$BUILD_TYPE" == "all" ]]; then
        extract_wheel "cr.ray.io/rayproject/ray-cpp-wheel-py${PYTHON_VERSION}:latest" "ray-cpp"
    fi

    # Clean up non-wheel files
    find .whl -type f ! -name '*.whl' -delete 2>/dev/null || true
    find .whl -type d -empty -delete 2>/dev/null || true

    header "Wheels extracted to .whl/:"
    ls -la .whl/*.whl 2>/dev/null || echo "    (no wheel files found)"
fi

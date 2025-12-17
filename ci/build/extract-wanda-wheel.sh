#!/bin/bash
#
# Extract Ray wheels from wanda-cached images.
#
# This script pulls the wheel scratch images from the wanda cache and extracts
# the .whl files to the .whl/ directory for upload by copy_build_artifacts.sh.
#
# Usage:
#   ./ci/build/extract-wanda-wheel.sh PYTHON_VERSION ARCH
#
# Arguments:
#   PYTHON_VERSION  - Python version (e.g., "3.10", "3.11", "3.12")
#   ARCH            - Architecture (e.g., "x86_64", "aarch64")
#
# Environment variables (set by CI):
#   RAYCI_WORK_REPO   - ECR repository for wanda cache
#   RAYCI_BUILD_ID    - Build ID prefix for wanda cache tags
#
# Example:
#   ./ci/build/extract-wanda-wheel.sh 3.10 x86_64
#
set -exuo pipefail

PYTHON_VERSION="${1:?Python version required (e.g., 3.10)}"
ARCH="${2:?Architecture required (e.g., x86_64)}"

# Determine architecture suffix for wanda image names
if [[ "$ARCH" == "x86_64" ]]; then
    ARCH_SUFFIX=""
elif [[ "$ARCH" == "aarch64" ]]; then
    ARCH_SUFFIX="-aarch64"
else
    echo "Error: Unknown architecture: $ARCH"
    exit 1
fi

# Wanda cache configuration
RAYCI_WORK_REPO="${RAYCI_WORK_REPO:-029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp}"
RAYCI_BUILD_ID="${RAYCI_BUILD_ID:?RAYCI_BUILD_ID environment variable required}"

# Wanda image names (must match the 'name:' field in *.wanda.yaml files)
RAY_WHEEL_IMAGE="ray-wheel-py${PYTHON_VERSION}${ARCH_SUFFIX}"
RAY_CPP_WHEEL_IMAGE="ray-cpp-wheel-py${PYTHON_VERSION}${ARCH_SUFFIX}"

# Full image tags in wanda cache
RAY_WHEEL_TAG="${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${RAY_WHEEL_IMAGE}"
RAY_CPP_WHEEL_TAG="${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${RAY_CPP_WHEEL_IMAGE}"

echo "=== Extracting Ray wheels from wanda cache ==="
echo "Python version: ${PYTHON_VERSION}"
echo "Architecture: ${ARCH} (suffix: '${ARCH_SUFFIX}')"
echo "Ray wheel image: ${RAY_WHEEL_TAG}"
echo "Ray C++ wheel image: ${RAY_CPP_WHEEL_TAG}"

# Create output directory
mkdir -p .whl

# Function to extract wheel from a scratch image
extract_wheel() {
    local image_tag="$1"
    local image_name="$2"

    echo "--- Extracting from ${image_name} ---"

    # Pull the image (scratch images are small, just contain .whl files)
    echo "Pulling ${image_tag}..."
    docker pull "${image_tag}"

    # Create a container from the image (scratch images need a dummy command)
    container_id=$(docker create "${image_tag}" /bin/true 2>/dev/null || docker create "${image_tag}" true)

    # Extract all .whl files from the root of the container
    # Scratch images have wheels at / (root)
    echo "Extracting wheels from container ${container_id}..."
    docker cp "${container_id}:/" - | tar -xf - -C .whl --wildcards '*.whl' 2>/dev/null || \
        docker cp "${container_id}:/" .whl/ 2>/dev/null || true

    # Clean up container
    docker rm "${container_id}" > /dev/null

    echo "Extracted from ${image_name}"
}

# Extract ray wheel
extract_wheel "${RAY_WHEEL_TAG}" "${RAY_WHEEL_IMAGE}"

# Extract ray-cpp wheel
extract_wheel "${RAY_CPP_WHEEL_TAG}" "${RAY_CPP_WHEEL_IMAGE}"

# Clean up any non-wheel files that might have been extracted
find .whl -type f ! -name '*.whl' -delete 2>/dev/null || true
find .whl -type d -empty -delete 2>/dev/null || true

echo "=== Extraction complete ==="
echo "Wheels in .whl/:"
ls -la .whl/*.whl 2>/dev/null || echo "(no wheel files found)"

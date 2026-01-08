#!/bin/bash

set -exuo pipefail

# Extract wheels from a Wanda-cached image.
#
# Usage: ./ci/build/extract_wanda_wheels.sh <wanda_image_name>
#
# Example:
#   ./ci/build/extract_wanda_wheels.sh ray-wheel-py3.10
#
# The script will:
#   1. Pull the wanda image from ECR cache
#   2. Extract .whl files from the image
#   3. Move them to .whl/ directory

WANDA_IMAGE_NAME=${1:?Usage: $0 <wanda_image_name>}

# Construct full image tag from environment
WANDA_IMAGE="${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${WANDA_IMAGE_NAME}"

echo "Extracting wheels from: ${WANDA_IMAGE}"

# Create container from wanda image (without running it)
container_id=$(docker create "${WANDA_IMAGE}")

# Extract wheels to temp directory
mkdir -p /tmp/wheels
docker cp "${container_id}":/ /tmp/wheels/

# Clean up container
docker rm "${container_id}"

# Move wheels to .whl directory
mkdir -p .whl
mv /tmp/wheels/*.whl .whl/

echo "Extracted wheels:"
ls -la .whl/

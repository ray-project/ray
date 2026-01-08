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
#   3. Move them to .whl/ directory (clears existing wheels first)

WANDA_IMAGE_NAME=${1:?Usage: $0 <wanda_image_name>}

# Construct full image tag from environment
WANDA_IMAGE="${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${WANDA_IMAGE_NAME}"

echo "Extracting wheels from: ${WANDA_IMAGE}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir" || true' EXIT

docker pull "${WANDA_IMAGE}"

# Image has no default CMD, so provide a dummy command.
container_id="$(docker create "${WANDA_IMAGE}" /no-such-cmd)"

# Copy wheel files directly from container (wanda wheel images have wheels at /)
docker cp "${container_id}":/ "${tmpdir}/"
docker rm "${container_id}"

# Clear existing wheels to avoid stale files from previous runs
rm -rf .whl
mkdir -p .whl

# Move extracted wheels (handles nested paths if any)
find "${tmpdir}" -type f -name '*.whl' -exec mv {} .whl/ \;

# Verify that wheels were actually extracted
shopt -s nullglob
wheels=(.whl/*.whl)
shopt -u nullglob

if (( ${#wheels[@]} == 0 )); then
  echo "ERROR: No wheel files were extracted from image: ${WANDA_IMAGE}" >&2
  echo "This may indicate image corruption, incorrect image tag, or path issues." >&2
  exit 1
fi

echo "Extracted ${#wheels[@]} wheel(s):"
ls -la .whl/

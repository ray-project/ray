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
#   1. Export the wanda image using crane
#   2. Extract .whl files from the tarball
#   3. Move them to .whl/ directory (clears existing wheels first)

WANDA_IMAGE_NAME=${1:?Usage: $0 <wanda_image_name>}

# Construct full image tag from environment
WANDA_IMAGE="${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-${WANDA_IMAGE_NAME}"

echo "Extracting wheels from: ${WANDA_IMAGE}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir" || true' EXIT

# Export image and extract to temp dir
crane export "${WANDA_IMAGE}" - | tar -xf - -C "$tmpdir"

# Clear existing wheels to avoid stale files from previous runs
rm -rf .whl
mkdir -p .whl

# Move extracted wheels to .whl/
find "${tmpdir}" -type f -name '*.whl' -exec mv {} .whl/ \;

# Verify that wheels were actually extracted
wheels=($(find .whl -maxdepth 1 -name '*.whl'))
if (( ${#wheels[@]} == 0 )); then
  echo "ERROR: No wheel files were extracted from image: ${WANDA_IMAGE}" >&2
  echo "This may indicate image corruption, incorrect image tag, or path issues." >&2
  exit 1
fi

echo "Extracted ${#wheels[@]} wheel(s):"
ls -la .whl/

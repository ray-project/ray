#!/bin/bash
set -euo pipefail

WHEEL_NAME="$1"
SOURCE_IMAGE="$2"
CONSTRAINTS_FILE="$3"
DEST_IMAGE="$4"
PIP_FREEZE_FILE="$5"

CPU_TMP="$(mktemp -d)"

cp -r .whl "${CPU_TMP}/.whl"
cp docker/ray/Dockerfile "${CPU_TMP}/Dockerfile"
cp python/requirements_compiled.txt "${CPU_TMP}/."

# Build the image.
cd "${CPU_TMP}"
tar --mtime="UTC 2020-01-01" -c -f - . \
    | docker build --progress=plain \
        --build-arg FULL_BASE_IMAGE="$SOURCE_IMAGE" \
        --build-arg WHEEL_PATH=".whl/${WHEEL_NAME}" \
        --build-arg CONSTRAINTS_FILE="$CONSTRAINTS_FILE" \
        -t "$DEST_IMAGE" -f Dockerfile -

# Copy the pip freeze file to the artifact mount.
mkdir -p /artifact-mount/.image-info
CONTAINER="$(docker create "$DEST_IMAGE")"
docker cp "$CONTAINER":/home/ray/pip-freeze.txt \
    /artifact-mount/.image-info/"$PIP_FREEZE_FILE"

#!/bin/bash
set -exuo pipefail

WHEEL_NAME="$1"
SOURCE_IMAGE="$2"
CONSTRAINTS_FILE="$3"
DEST_IMAGE="$4"

CPU_TMP="$(mktemp -d)"

mkdir -p "${CPU_TMP}/.whl"

cp .whl/"$WHEEL_NAME" "${CPU_TMP}/.whl/${WHEEL_NAME}"
cp docker/ray/Dockerfile "${CPU_TMP}/Dockerfile"
cp python/requirements_compiled.txt "${CPU_TMP}/."
cp python/requirements_compiled_py37.txt "${CPU_TMP}/."

cd "${CPU_TMP}"
tar --mtime="UTC 2020-01-01" -c -f - . \
    | docker build --progress=plain \
        --build-arg FULL_BASE_IMAGE="$SOURCE_IMAGE" \
        --build-arg WHEEL_PATH=".whl/${WHEEL_NAME}" \
        --build-arg CONSTRAINTS_FILE="$CONSTRAINTS_FILE" \
        -t "$DEST_IMAGE" -f Dockerfile -

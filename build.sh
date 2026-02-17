#!/bin/bash
# build.sh — Build the conda-forge reproducer image
#
# Usage:
#   ./build.sh                        # conda-forge ray (reproduces the bug)
#   ./build.sh --wheel .whl/ray*.whl  # replace ray with a local wheel (for testing fixes)
#
# The --wheel variant installs a local wheel on top of the conda-forge image,
# replacing conda-forge ray but keeping conda-forge grpcio/protobuf.
# Build the wheel first:
#   ./build-wheel.sh 3.11
#
# Must be run on a native amd64 machine.

set -euo pipefail

IMAGE_NAME="conda-ray-repro"

echo "=== Building conda-forge reproducer image ==="
docker build --platform linux/amd64 -t "$IMAGE_NAME" .
echo ""
echo "Built: $IMAGE_NAME"

if [ "${1:-}" = "--wheel" ]; then
    if [ -z "${2:-}" ]; then
        echo "ERROR: --wheel requires a path to a .whl file."
        echo "Usage: ./build.sh --wheel .whl/ray-*.whl"
        exit 1
    fi
    WHEEL_PATH="$2"
    if [ ! -f "$WHEEL_PATH" ]; then
        echo "ERROR: Wheel not found: $WHEEL_PATH"
        echo "Run './build-wheel.sh 3.11' first."
        exit 1
    fi

    WHEEL_DIR="$(cd "$(dirname "$WHEEL_PATH")" && pwd)"
    WHEEL_NAME="$(basename "$WHEEL_PATH")"
    echo ""
    echo "=== Layering local wheel: $WHEEL_NAME ==="
    docker build --platform linux/amd64 -t "$IMAGE_NAME" -f - "$WHEEL_DIR" <<EOF
FROM $IMAGE_NAME
COPY --chown=ray:users $WHEEL_NAME /tmp/$WHEEL_NAME
RUN /home/ray/micromamba/envs/ray-env/bin/pip install --no-cache-dir --no-deps --force-reinstall /tmp/$WHEEL_NAME && rm /tmp/$WHEEL_NAME
EOF
    echo ""
    echo "Updated: $IMAGE_NAME (with local wheel: $WHEEL_NAME)"
fi

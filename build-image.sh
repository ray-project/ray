#!/bin/bash
# build-image.sh - Build Ray Docker images locally
#
# This is a thin wrapper that delegates to ci/build/build_image.py via uv.
# Run with --help for usage information.

set -euo pipefail

# Check for uv
if ! command -v uv &>/dev/null; then
    echo "Error: uv is required. Install from: https://docs.astral.sh/uv/" >&2
    exit 1
fi

RAY_ROOT="$(git rev-parse --show-toplevel)"
RAYCI_VERSION=$(cat "$RAY_ROOT/.rayciversion")

# Detect platform for wheel tag
case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)                WHEEL_PLATFORM="macosx_12_0_arm64" ;;
    Linux-x86_64 | Linux-amd64)  WHEEL_PLATFORM="manylinux2014_x86_64" ;;
    Linux-aarch64 | Linux-arm64) WHEEL_PLATFORM="manylinux2014_aarch64" ;;
    *) echo "Error: Unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

RAYMAKE_URL="https://github.com/ray-project/rayci/releases/download/v${RAYCI_VERSION}/raymake-${RAYCI_VERSION}-py3-none-${WHEEL_PLATFORM}.whl"

PYTHONPATH="$RAY_ROOT${PYTHONPATH:+:$PYTHONPATH}" exec uv run --with "$RAYMAKE_URL" "$RAY_ROOT/ci/build/build_image.py" "$@"

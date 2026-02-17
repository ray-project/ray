#!/bin/bash
# test-repro.sh — Run the reproducer against an Anyscale workspace cluster
#
# Prerequisites:
#   1. anyscale login
#   2. A running workspace (either BYOD with conda-forge ray, or standard pip ray)
#   3. Find the workspace cluster name: anyscale cluster list --project <project>
#
# Usage:
#   export RAY_ADDRESS="anyscale://<project>/<workspace-cluster-name>"
#   export ANYSCALE_CLOUD="<cloud-name>"
#   export IGNORE_VERSION_CHECK=1
#   ./test-repro.sh

set -euo pipefail

if [ -z "${RAY_ADDRESS:-}" ]; then
    echo "ERROR: RAY_ADDRESS is not set."
    echo ""
    echo "Usage:"
    echo "  export RAY_ADDRESS=\"anyscale://<project>/<workspace-cluster-name>\""
    echo "  export ANYSCALE_CLOUD=\"<cloud-name>\""
    echo "  export IGNORE_VERSION_CHECK=1"
    echo "  ./test-repro.sh"
    echo ""
    echo "Find workspace cluster names with: anyscale cluster list --project <project>"
    exit 1
fi

# Env vars to pass into the container.
EXTRA_ARGS=()
[ -n "${ANYSCALE_CLOUD:-}" ] && EXTRA_ARGS+=(-e ANYSCALE_CLOUD="$ANYSCALE_CLOUD")
[ -n "${IGNORE_VERSION_CHECK:-}" ] && EXTRA_ARGS+=(-e IGNORE_VERSION_CHECK="$IGNORE_VERSION_CHECK")

# Anyscale credentials.
ANYSCALE_CREDS_DIR="${HOME}/.anyscale"
if [ -n "${ANYSCALE_CLI_TOKEN:-}" ]; then
    CREDS_ARGS=(-e ANYSCALE_CLI_TOKEN="$ANYSCALE_CLI_TOKEN")
    [ -n "${ANYSCALE_HOST:-}" ] && CREDS_ARGS+=(-e ANYSCALE_HOST="$ANYSCALE_HOST")
elif [ -d "$ANYSCALE_CREDS_DIR" ]; then
    CREDS_ARGS=(-v "$ANYSCALE_CREDS_DIR:/home/ray/.anyscale:ro")
else
    echo "ERROR: No Anyscale credentials found."
    echo ""
    echo "Either:"
    echo "  1. Run 'anyscale login' first (creates ~/.anyscale/credentials.json)"
    echo "  2. Set ANYSCALE_CLI_TOKEN env var"
    exit 1
fi

IMAGE="conda-ray-repro"

echo "=== Connecting to: $RAY_ADDRESS ==="
docker run --rm \
    -e RAY_ADDRESS="$RAY_ADDRESS" \
    "${CREDS_ARGS[@]}" \
    "${EXTRA_ARGS[@]}" \
    "$IMAGE" \
    bash -c 'eval "$(/usr/local/bin/micromamba shell hook --shell bash)" && \
             micromamba activate ray-env && \
             python /home/ray/reproduce_502.py'

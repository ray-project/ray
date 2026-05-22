#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    ffmpeg \
    && sudo rm -rf /var/lib/apt/lists/*

uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

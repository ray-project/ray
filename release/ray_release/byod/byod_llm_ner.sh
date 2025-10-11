#!/bin/bash

set -exo pipefail

# Will use lockfile instead later
# pip3 install --no-cache-dir -r https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/lockfile.txt

# Install Python dependencies
uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

# Env vars
export HF_HUB_ENABLE_HF_TRANSFER=1

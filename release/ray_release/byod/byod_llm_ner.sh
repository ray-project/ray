#!/bin/bash

set -exo pipefail

# Will use lockfile instead later
# pip3 install --no-cache-dir -r https://raw.githubusercontent.com/anyscale/e2e-llm-workflows/refs/heads/main/lockfile.txt

# Install Python dependencies
uv pip sync python/deplocks/examples/entity-recognition-with-llms/llm_example_py311_cu128.lock

# Env vars
export HF_HUB_ENABLE_HF_TRANSFER=1

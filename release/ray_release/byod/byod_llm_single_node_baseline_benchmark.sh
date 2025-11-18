#!/bin/bash

set -exo pipefail

uv pip install --system --no-cache-dir --no-deps \
    --index-strategy unsafe-best-match -r python_depset.lock

uv pip install --system datasets==4.4.1

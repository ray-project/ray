#!/bin/bash

set -exo pipefail

# Install Python dependencies
uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

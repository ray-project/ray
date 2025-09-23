#!/bin/bash

# Set bash options for safer script execution:
# -e: Exit immediately if any command fails
# -x: Print each command before executing it (for debugging)
# -o pipefail: Fail if any command in a pipeline fails (not just the last one)
set -exo pipefail

# Install Python dependencies.

uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

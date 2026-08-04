#!/bin/bash

set -exo pipefail

# Install Python dependencies
uv pip install -r "$HOME"/python_depset.lock --system --no-deps --index-strategy unsafe-best-match

# Run the test
python hello_world.py

#!/bin/bash

set -exo pipefail

# Install python dependencies
uv pip install -r requirements.in -c python_depset.lock --system --no-deps --index-strategy unsafe-best-match

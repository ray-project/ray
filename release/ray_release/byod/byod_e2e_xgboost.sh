#!/bin/bash

set -exo pipefail

uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

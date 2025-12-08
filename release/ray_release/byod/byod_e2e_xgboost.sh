#!/bin/bash

set -exo pipefail

uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

unset RAY_DATA_EXECUTION_CALLBACKS

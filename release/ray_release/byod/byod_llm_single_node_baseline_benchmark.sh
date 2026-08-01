#!/bin/bash

set -exo pipefail

uv pip install --system --no-deps --index-strategy unsafe-best-match -r python_depset.lock

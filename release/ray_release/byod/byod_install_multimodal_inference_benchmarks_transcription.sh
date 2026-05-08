#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

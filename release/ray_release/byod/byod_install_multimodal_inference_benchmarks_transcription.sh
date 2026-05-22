#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

conda install -y -c conda-forge "ffmpeg=7.*"

uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

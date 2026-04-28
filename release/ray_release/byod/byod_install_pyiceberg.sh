#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

uv pip install --system --index-strategy unsafe-best-match "pyiceberg[glue,s3fs]==0.11.0"

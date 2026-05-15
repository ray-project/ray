#!/bin/bash

set -euo pipefail

CONSTRAINTS_FILE="/tmp/ray-deps/requirements_compiled_py3.13.txt"
OUTPUT_FILE="/tmp/ray-deps/requirements_compiled_py3.13_no_ptl.txt"

if [[ -f "$CONSTRAINTS_FILE" ]]; then
    sed -e '/^pytorch-lightning/d' -e '/^numpy/d' "$CONSTRAINTS_FILE" > "$OUTPUT_FILE"
fi

# Strip lightning>=2 from tune-test-requirements so lightning1 back-compat
# build only has pytorch-lightning==1.8.6 (v1), not the v2 lightning package.
TUNE_TEST_REQ="python/requirements/ml/py313/tune-test-requirements.txt"
if [[ -f "$TUNE_TEST_REQ" ]]; then
    sed '/^lightning/d' "$TUNE_TEST_REQ" > /tmp/ray-deps/tune-test-requirements-no-lightning.txt
fi

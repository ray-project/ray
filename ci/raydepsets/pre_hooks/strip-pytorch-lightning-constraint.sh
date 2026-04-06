#!/bin/bash

set -euo pipefail

CONSTRAINTS_FILE="/tmp/ray-deps/requirements_compiled_py3.13.txt"
OUTPUT_FILE="/tmp/ray-deps/requirements_compiled_py3.13_no_ptl.txt"

if [[ -f "$CONSTRAINTS_FILE" ]]; then
    sed -e '/^pytorch-lightning/d' -e '/^numpy/d' "$CONSTRAINTS_FILE" > "$OUTPUT_FILE"
fi

#!/bin/bash

set -euo pipefail

CONSTRAINTS_FILE="/tmp/ray-deps/requirements_compiled_py3.13.txt"

if [[ -f "$CONSTRAINTS_FILE" ]]; then
    sed -i.bak '/^pytorch-lightning/d' "$CONSTRAINTS_FILE"
    rm -f "${CONSTRAINTS_FILE}.bak"
fi

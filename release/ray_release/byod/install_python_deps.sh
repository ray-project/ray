#!/bin/bash

set -euo pipefail

LOCK_FILE="${1:-python_depset.lock}"

if [[ ! -f "${LOCK_FILE}" ]]; then
    echo "Lock file ${LOCK_FILE} does not exist" >/dev/stderr
    exit 1
fi

uv pip install --system --no-deps --index-strategy unsafe-best-match \
    -r "${LOCK_FILE}"

#!/bin/bash

set -euo pipefail

S3_TEMP="s3://bk-premerge-first-jawfish-artifacts/tmp/runtime/${RAYCI_BUILD_ID}"

WHEELS=(.whl/*.whl)
for WHEEL in "${WHEELS[@]}"; do
    echo "Uploading ${WHEEL}"
    WHEEL_BASENAME="$(basename "${WHEEL}")"
    aws s3 cp "${WHEEL}" "${S3_TEMP}/${WHEEL_BASENAME}"
done

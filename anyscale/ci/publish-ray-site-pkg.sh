#!/bin/bash

set -euo pipefail

source anyscale/ci/setup-env.sh

TMP="$(mktemp -d)"

PY_VERSION_CODES=(py38 py39 py310 py311)

# Download and check ray-oss site package files.
for RAY_VAR in ray-oss ray-opt ; do
    for PY_VERSION_CODE in "${PY_VERSION_CODES[@]}"; do
        mkdir -p "${TMP}/${RAY_VAR}/${PY_VERSION_CODE}"
        cd "${TMP}/${RAY_VAR}/${PY_VERSION_CODE}"
        (
            aws s3 sync "${S3_TEMP}/${RAY_VAR}/${PY_VERSION_CODE}" .
            sha256sum ./*.tar.gz | tee sums.txt
            COUNT="$(awk '{print $1}' sums.txt | sort | uniq | wc -l)"
            if [[ "${COUNT}" != "1" ]]; then
                echo "Digest mismatch for ${RAY_VAR} ${PY_VERSION_CODE}: ${COUNT} unique digests" >/dev/stderr
                exit 1
            fi
        )
    done
done

# TODO(aslonnie): if this is on release branch, upload the site package to dev
# org data bucket.

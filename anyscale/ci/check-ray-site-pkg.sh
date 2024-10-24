#!/bin/bash

set -euo pipefail

source anyscale/ci/setup-env.sh

PY_VERSION_CODES=(py39 py310 py311 py312)

TMP="$(mktemp -d)"

# Download and check ray-oss site package files.
for RAY_VAR in ray-oss ray-opt ; do
    for PY_VERSION_CODE in "${PY_VERSION_CODES[@]}"; do
        mkdir -p "${TMP}/${RAY_VAR}/${PY_VERSION_CODE}"
        (
            cd "${TMP}/${RAY_VAR}/${PY_VERSION_CODE}"

            aws s3 sync "${S3_TEMP}/${RAY_VAR}/${PY_VERSION_CODE}" .
            sha256sum ./*.tar.gz | tee sums.txt

            for IS_MINIMIZED in "true" "false" ; do
              if [[ "${IS_MINIMIZED}" == "true" ]]; then
                  COUNT="$(awk '/min/{print $1}' sums.txt | sort | uniq | wc -l)"
              else
                  COUNT="$(awk '!/min/{print $1}' sums.txt | sort | uniq | wc -l)"
              fi
              if [[ "${COUNT}" != "1" ]]; then
                echo "Digest mismatch for ${RAY_VAR} ${PY_VERSION_CODE} (minimized: ${IS_MINIMIZED}): ${COUNT} unique digests" >/dev/stderr
                  exit 1
              fi
            done
        )
    done
done

# Cleanup temp dir.
rm -rf "${TMP}"

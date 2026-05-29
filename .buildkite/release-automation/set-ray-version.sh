#!/bin/bash

set -euo pipefail

if [[ -z "${RAY_VERSION:-}" ]]; then
    RAY_VERSION="$(python3 python/ray/_version.py | cut -d' ' -f1)"
    if [[ "${RAY_VERSION}" =~ dev0 ]]; then
        echo "RAY_VERSION is not set, value is ${RAY_VERSION}" >/dev/stderr
        exit 1
    fi
fi

if [[ "${RAY_COMMIT:-}" == "" ]]; then
    if [[ "${BUILDKITE_COMMIT:-}" == "" ]]; then
        echo "neither BUILDKITE_COMMIT nor RAY_COMMIT is set" >/dev/stderr
        exit 1
    fi
    RAY_COMMIT="${BUILDKITE_COMMIT:-}"
fi

export RAY_VERSION
export RAY_COMMIT

# Print it out for information.
echo "RAY_VERSION=$RAY_VERSION"
echo "RAY_COMMIT=$RAY_COMMIT"

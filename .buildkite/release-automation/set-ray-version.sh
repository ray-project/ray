#!/bin/bash

set -euo pipefail

if [[ -z "${RAY_VERSION:-}" ]]; then
    RAY_VERSION="$(python python/ray/_version.py | cut -d' ' -f1)"
    if [[ "${RAY_VERSION}" =~ dev0 ]]; then
        echo "RAY_VERSION is not set, value is ${RAY_VERSION}" >/dev/stderr
        exit 1
    fi
fi

export RAY_VERSION

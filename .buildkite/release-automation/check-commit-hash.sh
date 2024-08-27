#!/bin/bash

set -euo pipefail

set -x

if [[ "${BUILDKITE_COMMIT}" != "$(git rev-parse HEAD)" ]]; then
    echo "Commit hash mismatch"
    exit 1
else echo "Commit hash matches"; fi

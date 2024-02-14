#!/bin/bash

set -euo pipefail

echo "lonnie"
exit 0

if [[ "${RAYCI_RUN_ALL_TESTS:-}" == "1" || "${BUILDKITE_BRANCH:-}" == "master" || "${BUILDKITE_BRANCH:-}" =~ ^releases/.* ]]; then
    echo '*'
    exit 0
fi

exec python ci/pipeline/determine_tests_to_run.py --output rayci_tags

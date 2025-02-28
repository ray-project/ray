#!/bin/bash

set -euo pipefail

curl -sfL "https://raw.githubusercontent.com/ray-project/rayci/stable/run_rayci.sh" > /tmp/run_rayci.sh

if [[ "${BUILDKITE_BRANCH:-}" == "master" || "${BUILDKITE_BRANCH:-}" =~ ^releases/.* ]]; then
    /bin/bash /tmp/run_rayci.sh -upload -config anyscale/ci/config-postmerge.yaml
else
    /bin/bash /tmp/run_rayci.sh -upload -config anyscale/ci/config-premerge.yaml
fi

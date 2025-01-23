#!/bin/bash

set -euo pipefail

curl -sfL "https://raw.githubusercontent.com/ray-project/rayci/stable/run_rayci.sh" > /tmp/run_rayci.sh
if [[ "$BUILDKITE_PIPELINE_ID" == "019490b9-2fc3-4200-81f8-9bfa7fc383ab" ]]; then # new rayturbo pipeline
    /bin/bash /tmp/run_rayci.sh -upload -config anyscale/ci/config_2025.yaml
else # old rayturbo pipeline
    /bin/bash /tmp/run_rayci.sh -upload -config anyscale/ci/config.yaml
fi

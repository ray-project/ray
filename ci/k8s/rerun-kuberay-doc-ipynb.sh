#!/bin/bash

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/test-constants.sh"

cd doc/source/cluster/kubernetes

for test in "${DOC_TESTS[@]}"; do
  echo "Rerun notebook: ${test}"
  jupyter nbconvert --to notebook --inplace --execute \
    --ExecutePreprocessor.timeout=600 \
    --ExecutePreprocessor.kernel_name=bash \
    "${test}" || {
      echo "Run failed: ${test}"
      exit 1
    }
done

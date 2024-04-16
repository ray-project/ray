#!/bin/bash

set -eux

# set up the release and bisect infra environment
bash release/gcloud_docker_login.sh release/aws2gce_iam.json 
export PATH="$(pwd)/google-cloud-sdk/bin:$PATH"

# running bisect
export BUILDKITE_MAX_RETRIES=0
pip install -e release/
cd release 
python ray_release/scripts/ray_bisect.py "$RAYCI_BISECT_TEST_NAME" \
  "$RAYCI_BISECT_PASSING_COMMIT" "$RAYCI_BISECT_FAILING_COMMIT" \
  --concurrency "$RAYCI_BISECT_CONCURRENCY" \
  --run-per-commit "$RAYCI_BISECT_RUN_PER_COMMIT"

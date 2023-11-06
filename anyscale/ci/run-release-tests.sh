#!/bin/bash

# run-release-test.sh bootstrap buildkite release test pipeline.

set -euo pipefail


if [[ "${BUILDKITE_COMMIT}" == "HEAD" ]]; then
    BUILDKITE_COMMIT="$(git rev-parse HEAD)"
    export BUILDKITE_COMMIT
fi

curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-441.0.0-linux-arm.tar.gz
tar -xf google-cloud-cli-441.0.0-linux-arm.tar.gz
./google-cloud-sdk/install.sh -q
PATH="$(pwd)/google-cloud-sdk/bin:$PATH"
export PATH
gcloud auth login --cred-file=release/aws2gce_runtime_iam.json --quiet
gcloud auth configure-docker us-west1-docker.pkg.dev --quiet
pip3 install --user -U pip
pip3 install --user -r release/requirements_buildkite.txt
pip3 install --user --no-deps -e release/
export RELEASE_QUEUE_DEFAULT="default"
export RELEASE_AWS_BUCKET="runtime-release-test-artifacts"
RAY_WANT_COMMIT_IN_IMAGE="$(cat .UPSTREAM)"
export RAY_WANT_COMMIT_IN_IMAGE
cd release
python3 ray_release/scripts/build_pipeline.py \
    --test-collection-file release/release_runtime_tests.yaml \
    --run-jailed-tests \
    --run-unstable-tests \
    --global-config runtime_config.yaml \
    | buildkite-agent pipeline upload

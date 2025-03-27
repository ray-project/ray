#!/bin/bash

# run-release-test.sh bootstrap buildkite release test pipeline.

set -euo pipefail

install_tools() {
    apt-get update
    apt-get upgrade -y
    DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl zip unzip sudo gnupg git tzdata

    # Add docker client APT repository
    mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
        $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
        sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    # Install packages
    apt-get update
    apt-get install -y \
        awscli docker-ce-cli build-essential python-is-python3 python3-pip
}


if [[ "${BUILDKITE_COMMIT}" == "HEAD" ]]; then
    BUILDKITE_COMMIT="$(git rev-parse HEAD)"
    export BUILDKITE_COMMIT
fi

install_tools
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 830883877497.dkr.ecr.us-west-2.amazonaws.com

curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-467.0.0-linux-x86_64.tar.gz
tar -xf google-cloud-cli-467.0.0-linux-x86_64.tar.gz
./google-cloud-sdk/install.sh -q
PATH="$(pwd)/google-cloud-sdk/bin:$PATH"
export PATH
gcloud auth login --cred-file=release/aws2gce_runtime_iam.json --quiet
gcloud auth configure-docker us-west1-docker.pkg.dev --quiet

PATH="$HOME/.local/bin:$PATH"
export PATH
pip3 install --user -U pip

# Strip the hashes from the constraint file
# TODO(aslonnie): use bazel run..
grep '==' release/requirements_buildkite.txt > /tmp/requirements_buildkite_nohash.txt
sed -i 's/ \\//' /tmp/requirements_buildkite_nohash.txt  # Remove ending slashes.
sed -i 's/\[.*\]//g' /tmp/requirements_buildkite_nohash.txt  # Remove extras.

pip3 install --user -e release/ -c /tmp/requirements_buildkite_nohash.txt

if [[ "${BUILDKITE_PIPELINE_ID}" == "0194d305-a31d-40b8-9ffd-122388f1f14e" ]]; then
    export RELEASE_QUEUE_DEFAULT="rayturbo_small_queue"
else
    export RELEASE_QUEUE_DEFAULT="ondemand"
fi
export RELEASE_AWS_BUCKET="runtime-release-test-artifacts"
RAY_WANT_COMMIT_IN_IMAGE="$(cat .UPSTREAM)"
export RAY_WANT_COMMIT_IN_IMAGE
cd release
if [[ "${BUILDKITE_BRANCH}" != "releases/"* && "${RAYCI_RUN_ALL_RELEASE_TEST:-0}" != "1" ]]; then
    python3 ray_release/scripts/build_pipeline.py \
        --test-collection-file release/release_runtime_tests.yaml \
        --run-jailed-tests \
        --run-unstable-tests \
        --global-config runtime_config.yaml \
        | buildkite-agent pipeline upload
else
    python3 ray_release/scripts/build_pipeline.py \
        --test-collection-file release/release_runtime_tests.yaml \
        --test-collection-file release/release_data_tests.yaml \
        --test-collection-file release/release_tests.yaml \
        --run-jailed-tests \
        --global-config runtime_config.yaml \
        | buildkite-agent pipeline upload
fi

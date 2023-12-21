#!/bin/bash

set -ex

# We already checked out once at /c/workdir, but needs to checkout
# for another time for the symlink thing.

git config --global core.symlinks true
git config --global core.autocrlf false
git clone /c/workdir ray
cd ray

git checkout -f "${BUILDKITE_COMMIT}"

export PYTHON="3.8"
export RAY_USE_RANDOM_PORTS="1"
export RAY_DEFAULT_BUILD="1"
export LC_ALL="en_US.UTF-8"
export LANG="en_US.UTF-8"
export BUILD="1"
export DL="1"

powershell ci/pipeline/fix-windows-container-networking.ps1
powershell ci/pipeline/fix-windows-bazel.ps1

readonly PIPELINE_POSTMERGE="0189e759-8c96-4302-b6b5-b4274406bf89"

cleanup() {
    if [[ "${BUILDKITE_PIPELINE_ID:-}" == "${PIPELINE_POSTMERGE}" ]]; then
        bash ./ci/build/upload_build_info.sh
    fi
}
trap cleanup EXIT

upload_wheels() {
    if [[ "${BUILDKITE_PIPELINE_ID:-}" != "${PIPELINE_POSTMERGE}" ]]; then
        exit 0
    fi

    pip install -q docker aws_requests_auth boto3
    python .buildkite/copy_files.py --destination branch_wheels --path python/dist
    if [[ "${BUILDKITE_BRANCH}" == "master" ]]; then
        python .buildkite/copy_files.py --destination wheels --path python/dist
    fi
}

if [[ "$1" == "wheels" ]]; then
    export WINDOWS_WHEELS=1
    bash ci/ci.sh init
    bash ci/ci.sh build
    upload_wheels
    exit 0
fi

# Run tests

conda init
bash ci/ci.sh init
bash ci/ci.sh build
export RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1

bash ci/ci.sh "$1"

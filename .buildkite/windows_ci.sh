#!/bin/bash

set -ex

# We already checked out once at /c/workdir, but needs to checkout
# for another time for the symlink thing.

git config --global core.symlinks true
git config --global core.autocrlf false
git clone /c/workdir ray
cd ray

git checkout -f "${BUILDKITE_COMMIT}"

export PYTHON="3.9"
export RAY_USE_RANDOM_PORTS="1"
export RAY_DEFAULT_BUILD="1"
export LC_ALL="en_US.UTF-8"
export LANG="en_US.UTF-8"
export BUILD="1"
export DL="1"

powershell ci/pipeline/fix-windows-container-networking.ps1
powershell ci/pipeline/fix-windows-bazel.ps1

cleanup() {
    bash ./ci/build/upload_build_info.sh
}
trap cleanup EXIT

if [[ "$1" == "wheels" ]]; then
    export WINDOWS_WHEELS=1
    bash ci/ci.sh init
    bash ci/ci.sh build
    bash ci/build/copy_build_artifacts.sh wheel
    exit 0
fi

# Run tests

conda init
bash ci/ci.sh init
bash ci/ci.sh build
export RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1

bash ci/ci.sh "$1"

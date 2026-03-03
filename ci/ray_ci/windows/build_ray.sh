#!/bin/bash

set -ex

git config --global core.symlinks true
git config --global core.autocrlf false
mkdir -p /c/rayci
git clone . /c/rayci
cd /c/rayci

{
  echo "build --announce_rc";
  echo "build --config=ci";
  # Set a shorter output_base to avoid long file paths that Windows can't handle.
  echo "startup --output_base=c:/bzl";
  echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}";
} >> ~/.bazelrc

if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
  # Do not upload cache results for premerge pipeline
  echo "build --remote_upload_local_results=false" >> ~/.bazelrc
fi

# Fix network for remote caching
powershell ci/pipeline/fix-windows-container-networking.ps1

# Build ray and ray wheels
pip install -v -e python
pip wheel -e python -w .whl

# Clean up caches to speed up docker build
bazel clean --expunge

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
  echo "startup --output_user_root=c:/raytmp";
  echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}";
} >> ~/.bazelrc

if [[ "$BUILDKITE_PIPELINE_ID" == "0189942e-0876-4b8f-80a4-617f988ec59b" ]]; then
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

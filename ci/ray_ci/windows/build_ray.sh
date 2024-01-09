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

# Fix network for remote caching
powershell ci/pipeline/fix-windows-container-networking.ps1

# Build ray and ray wheels
pip install -v -e python
pip wheel -e python -w .whl

# Clean up caches to speed up docker build
bazel clean --expunge

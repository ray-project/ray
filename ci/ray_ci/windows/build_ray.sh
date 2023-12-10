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
  echo "startup --output_user_root=c:/tmp";
  echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}";
} >> ~/.bazelrc
powershell ci/pipeline/fix-windows-container-networking.ps1

pip install -v -e python
pip wheel -e python -w .whl

# Clean up temp files to speed up docker build
bazel clean --expunge

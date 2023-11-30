#!/bin/bash

set -ex

git config --global core.symlinks true
git config --global core.autocrlf false
mkdir -p /c/rayci
git clone . /c/rayci
cd /c/rayci

powershell ci/pipeline/fix-windows-bazel.ps1

{
  echo "build --announce_rc";  
  echo "build --config=ci";
  echo "startup --output_user_root=c:/tmp";
  echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}";
} >> ~/.bazelrc

# Build ray
conda init 
pip install -U --ignore-installed  \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt
pip install -v -e python

# Clean up temp files to speed up docker build
pip cache purge
bazel clean --expunge
powershell ci/pipeline/fix-windows-recycle.ps1

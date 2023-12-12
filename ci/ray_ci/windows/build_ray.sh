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
  echo "startup --output_user_root=c:/ray-work";
  echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}";
} >> ~/.bazelrc

# Build ray
conda init 
pip install -U --no-cache-dir --ignore-installed  \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt
pip install --no-cache-dir torch==2.0.1 torchvision==0.15.2 \
  tensorflow==2.11.0 tensorflow-probability==0.19.0
pip install --no-cache-dir -v -e python
pip wheel -e python -w .whl

# Clean up temp files to speed up docker build
bazel clean --expunge
powershell ci/ray_ci/windows/cleanup.ps1

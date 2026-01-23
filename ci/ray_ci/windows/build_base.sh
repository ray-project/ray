#!/bin/bash

set -ex

export TORCH_VERSION=2.0.1
export TORCHVISION_VERSION=0.15.2

# Delete the existing bazel and replace it with bazelisk.
powershell ci/ray_ci/windows/install_bazelisk.ps1

# Install uv
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/0.9.22/install.ps1 | iex"

conda init
conda install -q -y python="${PYTHON_FULL_VERSION}" requests=2.32.3

# Install torch first, as some dependencies (e.g. torch-spline-conv) need torch to be
# installed for their own install.
pip install -U --ignore-installed -c python/requirements_compiled.txt torch torchvision
pip install -U --ignore-installed -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt \
  -r python/requirements/ml/dl-cpu-requirements.txt

# Clean up caches to minimize image size. These caches are not needed, and
# removing them help with the build speed.
pip cache purge
powershell ci/ray_ci/windows/cleanup.ps1

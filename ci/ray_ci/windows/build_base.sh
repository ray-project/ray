#!/bin/bash

set -ex

export TORCH_VERSION=2.0.1
export TORCHVISION_VERSION=0.15.2

# Delete the existing bazel and replace it with bazelisk.
powershell ci/ray_ci/windows/install_bazelisk.ps1

# Install uv
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/0.9.22/install.ps1 | iex"

conda init
# Include ca-certificates + certifi in this solve so the trust store is refreshed with
# Python/requests (ray-project/ray#61545). Avoid a follow-up `conda update -c conda-forge`:
# it can replace conda.exe in place on Windows and fail in Docker (PermissionError on
# conda.exe.c~; see conda/conda#15760).
# Use `conda install` (explicit specs) rather than `conda update` (broader "newest compatible" refresh/upgrade more of the environment including conda itself which can cause issues with the build).
conda install -q -y python="${PYTHON_FULL_VERSION}" requests=2.32.3 ca-certificates certifi

# Install torch first, as some dependencies (e.g. torch-spline-conv) need torch to be
# installed for their own install.
pip install -U --ignore-installed -c python/requirements_compiled.txt torch torchvision
pip install -U --ignore-installed -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt \
  -r python/requirements/ml/dl-cpu-requirements.txt

# Ensure urllib/requests see an up-to-date certifi bundle after constrained installs.
python -m pip install -U certifi

# Clean up caches to minimize image size. These caches are not needed, and
# removing them help with the build speed.
pip cache purge
powershell ci/ray_ci/windows/cleanup.ps1

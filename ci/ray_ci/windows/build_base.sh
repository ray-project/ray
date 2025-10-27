#!/bin/bash

set -ex

export TORCH_VERSION=2.0.1
export TORCHVISION_VERSION=0.15.2

# Delete the existing bazel and replace it with bazelisk.
powershell ci/ray_ci/windows/install_bazelisk.ps1

conda init
# newer requests version is needed for python 3.9+
conda install -q -y python="${PYTHON}" requests=2.31.0

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

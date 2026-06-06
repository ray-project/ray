#!/bin/bash

set -ex

export TORCH_VERSION=2.0.1
export TORCHVISION_VERSION=0.15.2

# Delete the existing bazel and replace it with bazelisk.
powershell ci/ray_ci/windows/install_bazelisk.ps1

# Install uv
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/0.9.22/install.ps1 | iex"

conda init

# Build the test interpreter in a dedicated conda env instead of swapping the
# base env's python in place. The base image ships Python 3.8 in `base`, and an
# in-place `conda install python=${PYTHON_FULL_VERSION}` either crashes
# (conda#15760) or -- with auto_update_conda disabled -- silently no-ops and
# leaves `base` on 3.8, which then mismatches the py310 bazel deps and blows up
# at test time. A fresh env sidesteps both failure modes. We put it first on
# PATH below for this build; windows.build.Dockerfile does the same on the
# image's machine PATH so bazel resolves to it at test time.
conda create -y -n "${RAY_CONDA_ENV}" python="${PYTHON_FULL_VERSION}" requests=2.32.3
# Force CA trust stack to the newest versions available at build time.
conda update -n "${RAY_CONDA_ENV}" -c conda-forge -y ca-certificates certifi

# Put the env first on PATH for the rest of this build so `python`/`pip` below
# resolve to it. (Test-time PATH is set on the image's machine PATH by
# windows.build.Dockerfile, because on Windows the base PATH lives in the
# registry, not in Docker ENV.)
env_root="/c/Miniconda3/envs/${RAY_CONDA_ENV}"
export PATH="${env_root}:${env_root}/Library/mingw-w64/bin:${env_root}/Library/usr/bin:${env_root}/Library/bin:${env_root}/Scripts:${env_root}/bin:${PATH}"

# Fail the build loudly here if the interpreter on PATH is not the expected
# version, rather than letting the mismatch surface as a confusing import error
# (e.g. `module 'typing' has no attribute '_SpecialGenericAlias'`) at test time.
python --version
python -c "import sys; v = '%d.%d' % sys.version_info[:2]; exp = '${PYTHON}'; assert v == exp, f'expected python {exp} on PATH, got {sys.version}'"

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

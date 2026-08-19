#!/bin/bash

set -ex

# Delete the existing bazel and replace it with bazelisk.
powershell ci/ray_ci/windows/install_bazelisk.ps1

# Install uv
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/0.9.22/install.ps1 | iex"

conda init
# TODO(ci): Remove once conda fixes the splitext bug in delete.py (conda/conda#15760).
# Conda 26.3.1 crashes on Windows when it tries to clean up a locked exe during
# a build-variant swap. Preventing self-update avoids that code path.
conda config --set auto_update_conda false
conda install -q -y python="${PYTHON_FULL_VERSION}" requests=2.32.3 pyopenssl=23.2.0
# Force CA trust stack to the newest versions available at build time.
conda update --freeze-installed -c conda-forge -q -y ca-certificates certifi

# Install the Windows test environment from its raydepsets lock file (see
# ci/raydepsets/configs/ci_windows.depsets.yaml). The lock resolves
# python/requirements.txt, test-requirements.txt and dl-cpu-requirements.txt
# for windows/py3.10, with torch pinned back to 2.7.0: torch 2.8/2.9 Windows
# wheels are built without libuv and cannot initialize the gloo backend
# (pytorch/pytorch#150381; fixed upstream in torch 2.10).
# Strip hashes and install --no-deps, like ci/ray_ci/macos/macos_ci.sh: the
# lock is a complete closure, and hash mode would choke on the unhashed
# packages that are intentionally excluded from it (ray, setuptools).
sed 's/ \\$//; s/ --hash[^ ]*//g' \
  python/deplocks/ci/windows-tests-torch27-ci_depset_py3.10.lock \
  > /tmp/windows_tests_depset_no_hashes.txt
pip install -U --ignore-installed --no-deps -r /tmp/windows_tests_depset_no_hashes.txt

# The index proxy, so the package installs inside this image's containers resolve
# through the CI package mirror rather than files.pythonhosted.org (pypi/support#11895).
# Baked in here rather than installed per job because the Windows agent host carries
# Python 3.8, below what the proxy supports, while conda above supplies 3.10 -- which is
# enough, since the only dependency needing 3.11 is skipped by the installer and guarded
# at import. ci/ray_ci/windows/pypi_proxy.sh starts it and is sourced by the two places
# that install packages.
(
  cd ci &&
    RAYCI_PYPI_PROXY_PREFIX="/c/pypiproxy" \
      RAYCI_PYPI_PROXY_SKIP_PROFILE=1 \
      bash install_pypi_proxy.sh "$(command -v python)"
)

# Clean up caches to minimize image size. These caches are not needed, and
# removing them help with the build speed.
pip cache purge
powershell ci/ray_ci/windows/cleanup.ps1

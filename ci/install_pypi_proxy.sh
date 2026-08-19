#!/bin/bash
# Install the job-local PyPI index proxy into an image.
#
# Shared by every image whose steps resolve Python packages, so the install lives in
# one place rather than being copied per Dockerfile. Expects ci/pypi_index_proxy.py
# and ci/pypi_proxy_profile.sh to be present in the working directory, which the
# callers arrange with --mount=type=bind.
#
# $1 is a python >= 3.11: asgi-cross-origin-protection requires it, and images whose
# default interpreter is older pass a second one here rather than moving their
# default. The venv is self-contained in /opt/pypiproxy, so nothing else in the image
# resolves through it.

set -euo pipefail

PROXY_PYTHON="${1:?usage: install_pypi_proxy.sh <path-to-python3.11+>}"

"$PROXY_PYTHON" -m venv /opt/pypiproxy
/opt/pypiproxy/bin/pip install --no-cache-dir \
  "asgi-cross-origin-protection==0.1.1" \
  "niquests==3.21.0" \
  "starlette==1.6.0" \
  "uvicorn==0.52.3"
cp pypi_index_proxy.py /opt/pypiproxy/pypi_index_proxy.py

# Sourced automatically: CI steps run under `bash -elic`, a login shell. The zz-
# prefix runs it after anything else in profile.d that might set HOME or PATH.
cp pypi_proxy_profile.sh /etc/profile.d/zz-rayci-pypi-proxy.sh
chmod 0644 /etc/profile.d/zz-rayci-pypi-proxy.sh

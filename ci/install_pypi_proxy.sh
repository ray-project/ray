#!/bin/bash
# Install the job-local PyPI index proxy.
#
# Shared by every image whose steps resolve Python packages, and by the agents that run
# steps with no image at all, so the install lives in one place rather than being copied
# per caller. Expects ci/pypi_index_proxy.py and ci/pypi_proxy_profile.sh to be present
# in the working directory, which image callers arrange with --mount=type=bind.
#
# $1 is the interpreter to build the venv from. Python 3.10 is enough; 3.11+ additionally
# gets the cross-origin middleware, which is the one dependency that requires it (see
# pypi_index_proxy.py). Images whose default interpreter is older pass a second one here
# rather than moving their default.
#
# RAYCI_PYPI_PROXY_PREFIX chooses where the venv lands, defaulting to /opt/pypiproxy.
# Agents that run steps directly on the host -- macOS, Windows -- point it somewhere
# writable without sudo, since there is no image build to run as root.
#
# RAYCI_PYPI_PROXY_SKIP_PROFILE=1 skips installing the profile.d hook, for the same
# hosts: they have no /etc/profile.d worth writing to and start the proxy explicitly.

set -euo pipefail

PROXY_PYTHON="${1:?usage: install_pypi_proxy.sh <path-to-python3.10+>}"
PREFIX="${RAYCI_PYPI_PROXY_PREFIX:-/opt/pypiproxy}"

# Refuse an interpreter the proxy cannot run on, rather than building a venv that fails
# at import time in the middle of a job.
"$PROXY_PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' || {
  echo "install_pypi_proxy: $PROXY_PYTHON is older than 3.10" >&2
  "$PROXY_PYTHON" --version >&2 || true
  exit 1
}

# --clear so a re-install repairs a broken venv rather than inheriting it. Without it,
# an attempt that failed partway -- a bad interpreter, an interrupted job -- leaves
# stale bin/ symlinks that a later attempt with a good interpreter keeps, and the
# result is a venv whose python cannot start at all.
"$PROXY_PYTHON" -m venv --clear "$PREFIX"

# starlette and uvicorn need 3.10, niquests 3.7, but asgi-cross-origin-protection needs
# 3.11 -- so on an older interpreter it is left out and the proxy guards its import. It
# only ever matters for non-GET routes, and every route here is a GET.
deps=("niquests==3.21.0" "starlette==1.6.0" "uvicorn==0.52.3")
if "$PROXY_PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
  deps+=("asgi-cross-origin-protection==0.1.1")
fi
"$PREFIX"/bin/pip install --no-cache-dir "${deps[@]}"

cp pypi_index_proxy.py "$PREFIX"/pypi_index_proxy.py
# The bazel downloader helper travels with the proxy, because the profile.d hook that
# calls it is installed into the image and cannot reach a checkout that does not exist
# yet at shell start.
cp bazel_mirror_downloader.sh "$PREFIX"/bazel_mirror_downloader.sh

if [[ "${RAYCI_PYPI_PROXY_SKIP_PROFILE:-0}" != "1" ]]; then
  # Sourced automatically: CI steps run under `bash -elic`, a login shell. The zz-
  # prefix runs it after anything else in profile.d that might set HOME or PATH.
  cp pypi_proxy_profile.sh /etc/profile.d/zz-rayci-pypi-proxy.sh
  chmod 0644 /etc/profile.d/zz-rayci-pypi-proxy.sh
fi

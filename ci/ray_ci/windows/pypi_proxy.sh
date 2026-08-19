#!/bin/bash
# Point this Windows container's pip and uv at the CI package mirror. Source it, do not
# run it: it exports the index variables into the calling shell.
#
# Unlike the Linux images there is no profile.d hook here, because the two places that
# install packages on Windows are not login shells: `RUN bash build_ray.sh` inside the
# docker build the tester starts, and the command list the wheel builder passes to
# `docker run`. Both source this explicitly.
#
# The proxy runs inside this container on loopback rather than on the agent. That avoids
# needing an interpreter on the Windows host, which carries 3.8 -- below what the proxy
# supports -- and avoids having to discover the host's address from inside a container.
# The venv is baked into the windowsbuild image by build_base.sh, so nothing is
# installed at job time.
#
# Fails open at every step: if the mirror is unreachable or the proxy will not start,
# nothing is exported and pip resolves from public PyPI exactly as before.

_rayci_windows_pypi_proxy() {
  local prefix="/c/pypiproxy"
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

  if [[ ! -x "${prefix}/Scripts/python.exe" ]]; then
    echo "pypi index: this image carries no proxy; resolving from public PyPI" >&2
    return 0
  fi

  # Loopback is correct here: this container is the only thing that needs to reach the
  # proxy, and pip and uv exempt loopback from their refusal to use a plain-HTTP index.
  export RAYCI_PYPI_PROXY_PREFIX="${prefix}"
  export RAYCI_PYPI_PROXY_PYTHON="${prefix}/Scripts/python.exe"
  export RAYCI_PYPI_PROXY_HOST="127.0.0.1"
  # shellcheck source=ci/pypi_proxy_profile.sh
  source "${repo_root}/ci/pypi_proxy_profile.sh"
}

_rayci_windows_pypi_proxy

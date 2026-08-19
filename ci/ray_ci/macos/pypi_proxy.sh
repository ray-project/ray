#!/bin/bash
# Point this macOS job's pip and uv at the CI package mirror. Source it, do not run it:
# it exports the index variables into the calling shell.
#
# Why macOS needs its own entry point rather than the profile.d hook the Linux images
# use: these steps run directly on the agent, not in a container, so there is no image
# to install the proxy into and no /etc/profile.d being sourced. What is here instead is
# the same two pieces -- install, then start -- called explicitly.
#
# Two properties of these agents make this cheap. They are long-lived (a launchd
# service, no disconnect-after-job) with a persistent HOME, so the venv below is built
# once per machine rather than once per job. And nothing else needs to reach the proxy,
# since no containers are involved, so it binds loopback -- which pip and uv accept over
# plain HTTP without any trusted-host handling.
#
# Every failure path leaves the environment untouched, so a job resolves from PyPI
# exactly as it did before. That includes the first install on a cold machine, which
# necessarily fetches from PyPI itself and can hit the very 502s this works around.

_rayci_macos_pypi_proxy() {
  local prefix="${HOME}/.pypiproxy"
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

  # Validate rather than test for the directory: a half-finished install from an
  # interrupted job would otherwise look complete and fail later, inside pip.
  if ! "${prefix}/bin/python" -c 'import niquests, starlette, uvicorn' 2>/dev/null; then
    # Newest first: the proxy only needs 3.10, and from 3.11 it also gets the
    # cross-origin middleware. python.org builds land in /Library/Frameworks -- the
    # reef userdata installs 3.9 and 3.10 there -- and miniforge supplies another 3.10.
    local candidate found=""
    for candidate in \
      /Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12 \
      /Library/Frameworks/Python.framework/Versions/3.11/bin/python3.11 \
      /Library/Frameworks/Python.framework/Versions/3.10/bin/python3.10 \
      /opt/homebrew/opt/miniforge/bin/python3 \
      "$(command -v python3.12 2>/dev/null)" \
      "$(command -v python3.11 2>/dev/null)" \
      "$(command -v python3.10 2>/dev/null)"; do
      if [[ -n "${candidate}" && -x "${candidate}" ]]; then
        found="${candidate}"
        break
      fi
    done
    if [[ -z "${found}" ]]; then
      echo "pypi index: no python >= 3.10 on this agent; resolving from public PyPI" >&2
      return 0
    fi

    echo "pypi index: installing the index proxy into ${prefix} with ${found}"
    # venv creation is idempotent, so a failed attempt is simply retried next job
    # rather than needing the directory cleared.
    if ! (
      cd "${repo_root}/ci" &&
        RAYCI_PYPI_PROXY_PREFIX="${prefix}" \
          RAYCI_PYPI_PROXY_SKIP_PROFILE=1 \
          bash install_pypi_proxy.sh "${found}"
    ); then
      echo "pypi index: proxy install failed; resolving from public PyPI" >&2
      return 0
    fi
  fi

  # The mirror probe, the mode selection, the readiness wait and the fail-open paths are
  # all shared with the Linux images rather than reimplemented here.
  export RAYCI_PYPI_PROXY_PREFIX="${prefix}"
  export RAYCI_PYPI_PROXY_HOST="127.0.0.1"
  # shellcheck source=ci/pypi_proxy_profile.sh
  source "${repo_root}/ci/pypi_proxy_profile.sh"
}

_rayci_macos_pypi_proxy

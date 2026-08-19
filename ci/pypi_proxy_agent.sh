#!/bin/bash
# Start the PyPI index proxy on the Buildkite agent, for the steps that no image can
# reach. Sourced from .buildkite/hooks/pre-command; it exports and must not be run.
#
# The images cover steps that run inside them: forge and manylinux each carry the proxy
# and start it from /etc/profile.d. What they cannot cover is anything that never enters
# a container -- the release pipeline's init step, and every wanda image build, which run
# directly on the agent. Those are the surfaces still resolving from files.pythonhosted.org
# (pypi/support#11895), and init failing means zero release tests run at all.
#
# Two addresses come out of this, because they serve different consumers:
#
#   PIP_INDEX_URL              127.0.0.1 -- for pip and uv running on the agent itself.
#                              Loopback, so pip and uv accept it over plain HTTP with no
#                              trusted-host handling.
#   RAYCI_IMAGE_PIP_INDEX_URL  the docker bridge gateway -- for docker builds, which have
#                              their own loopback and reach the agent here. wanda resolves
#                              build args from its own process environment, so exporting
#                              this is what lets an image build use the mirror.
#
# Everything is guarded: the hook runs under `set -e`, and a package mirror must never be
# the reason a job fails. Every path here leaves the environment untouched, and a step
# that gets nothing exported resolves from public PyPI exactly as it does today.

_rayci_agent_pypi_proxy() {
  [[ -n "${BUILDKITE:-}" ]] || return 0
  # profile.d in the images sets this; if a previous hook run already did the work, or a
  # step re-sources us, do not start a second copy.
  [[ -z "${RAYCI_PYPI_INDEX_MODE:-}" ]] || return 0

  local mirror="${RAYCI_PYPI_MIRROR_URL:-https://mirror.ci.ray.io}"
  local port="${RAYCI_PYPI_PROXY_PORT:-35999}"
  local prefix="${RAYCI_PYPI_PROXY_PREFIX:-/var/tmp/rayci-pypiproxy}"
  local log="/tmp/rayci_pypi_proxy_agent.log"
  : >"${log}" 2>/dev/null || true
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

  # Reachability first: on a fleet with no mirror deployed this is the only check that
  # runs, and it costs one request.
  if ! curl -sf -m 15 -o /dev/null "${mirror}/pypi.org/simple/pip/" 2>/dev/null; then
    echo "pypi index: mirror unreachable from this agent; resolving from public PyPI" >&2
    return 0
  fi

  # Bazel runs on the agent too, and its downloader is a separate mechanism from pip's
  # index: rules_python declares its bootstrap wheels as http_archive with literal
  # files.pythonhosted.org URLs, which no index setting reaches. The images have had this
  # since #65599 and the agent had nothing, so release 104848's init step still fetched
  # tomli-2.0.1 from the origin. Done here, before the proxy, because it needs only the
  # mirror -- and the probe above has just established that.
  # shellcheck source=ci/bazel_mirror_downloader.sh
  if source "${repo_root}/ci/bazel_mirror_downloader.sh" 2>/dev/null; then
    rayci_bazel_downloader_config "${mirror}"
  else
    echo "pypi index: no bazel downloader helper in this checkout; bazel stays on the origin" >&2
  fi

  # Validate rather than test for a directory: a half-finished install from an interrupted
  # job would otherwise look complete and fail later, inside pip.
  if ! "${prefix}/bin/python" -c 'import niquests, starlette, uvicorn' 2>/dev/null; then
    # RAYCI_PYPI_PROXY_PYTHON names the interpreter outright, for agents where the search
    # would not find a suitable one -- the Windows host carries 3.8, so it needs one
    # provisioned (uv python install) and named here.
    local candidate found="${RAYCI_PYPI_PROXY_PYTHON:-}"
    for candidate in python3.13 python3.12 python3.11 python3.10 python3; do
      [[ -z "${found}" ]] || break
      local path
      path="$(command -v "${candidate}" 2>/dev/null)" || continue
      if [[ -n "${path}" ]] && "${path}" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
        found="${path}"
        break
      fi
    done
    if [[ -z "${found}" ]]; then
      echo "pypi index: no python >= 3.10 on this agent; resolving from public PyPI" >&2
      return 0
    fi

    echo "pypi index: installing the index proxy into ${prefix} with ${found}"
    # This install is itself a PyPI fetch, so it can hit the very fault being worked
    # around. It fails open, which makes it no worse than not trying.
    if ! (
      cd "${repo_root}/ci" &&
        RAYCI_PYPI_PROXY_PREFIX="${prefix}" \
          RAYCI_PYPI_PROXY_SKIP_PROFILE=1 \
          bash install_pypi_proxy.sh "${found}"
    ) >>"${log}" 2>&1; then
      echo "pypi index: proxy install failed; resolving from public PyPI" >&2
      tail -n 20 "${log}" >&2 || true
      return 0
    fi
  fi

  # Bound on all interfaces so both consumers can reach it: the agent over loopback, and
  # docker builds over the bridge gateway.
  if ! curl -sf -m 5 -o /dev/null "http://127.0.0.1:${port}/healthz" 2>/dev/null; then
    if command -v setsid >/dev/null 2>&1; then
      MIRROR_URL="${mirror}" setsid "${prefix}/bin/python" \
        "${prefix}/pypi_index_proxy.py" "${port}" >>"${log}" 2>&1 &
    else
      ( MIRROR_URL="${mirror}" nohup "${prefix}/bin/python" \
          "${prefix}/pypi_index_proxy.py" "${port}" >>"${log}" 2>&1 & )
    fi
    local _
    for _ in $(seq 1 60); do
      curl -sf -m 5 -o /dev/null "http://127.0.0.1:${port}/healthz" 2>/dev/null && break
      sleep 0.5
    done
  fi

  # Probed through the proxy rather than at /healthz, which does not touch the mirror: a
  # process that is up but cannot reach its upstream must not be advertised.
  if ! curl -sf -m 20 -o /dev/null "http://127.0.0.1:${port}/simple/pip/" 2>/dev/null; then
    echo "pypi index: proxy did not serve an index; resolving from public PyPI" >&2
    tail -n 20 "${log}" >&2 || true
    return 0
  fi

  export RAYCI_PYPI_INDEX_MODE="agent-proxy"
  export PIP_INDEX_URL="http://127.0.0.1:${port}/simple"
  export UV_INDEX_URL="http://127.0.0.1:${port}/simple"
  # rules_python passes --isolated to whl_library's pip, which makes it ignore every PIP_*
  # variable. It reads this before deciding to pass the flag.
  export RULES_PYTHON_PIP_ISOLATED=0
  echo "pypi index: agent proxy over the mirror -> ${PIP_INDEX_URL}"

  # RAYCI_IMAGE_PIP_INDEX_URL is deliberately NOT exported here.
  #
  # It was, and it broke every wheel build on master: postmerge 19281 failed
  # `wanda: wheel py3.{10,11,12,13}` where 19280 passed, on the commit that added this
  # hook. pip inside the image build was pointed at the agent's bridge gateway, could not
  # reach it, and reported "Could not find a version that satisfies the requirement
  # cython==3.0.12 (from versions: none)" -- no 502, no origin fetch, nothing.
  #
  # Two things made that worse than a no-op. The gateway address is not the one a build
  # can reach: premerge 72149 measured a docker build reaching a *step container* on the
  # bridge (172.16.0.2), which is a different hop from reaching the *host* at the bridge
  # gateway, and the two were conflated. And the fail-open does not extend into an image
  # build: ray-wheel.Dockerfile reads ${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple},
  # so an absent value falls back to PyPI while a present-but-unreachable one fails hard.
  #
  # Restoring it needs the address a docker build can actually reach, verified from inside
  # a build on the agent that will run it, rather than inferred from `docker network
  # inspect bridge`.
}

_rayci_agent_pypi_proxy

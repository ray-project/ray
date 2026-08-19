# shellcheck shell=bash
# Installed as /etc/profile.d/zz-rayci-pypi-proxy.sh in the forge image.
#
# Points pip, uv and bazel at the CI package mirror when this job can reach it, and
# leaves them on public PyPI when it cannot. Steps run under `bash -elic`, a login
# shell, so this is sourced automatically and every step gets it without per-step
# wiring. The variables it exports are the ones .bazelrc forwards to repository
# rules and ci/ray_ci/container.py forwards into nested containers.
#
# Which index to use is decided by probing, not by assuming, because the mirror
# serves two different shapes and only one of them is usable directly:
#
#   1. A rewriting simple index at /simple. Its pages already point at the mirror
#      for the artifacts, so pip can use it as-is. Nothing local runs and nested
#      containers reach it by hostname.
#   2. A path-prefixed byte cache at /pypi.org/simple. Those pages are PyPI's own,
#      so their bodies still point at files.pythonhosted.org and using them as an
#      index leaves every wheel download on the origin that returns the 502s. In
#      that case start ci/pypi_index_proxy.py, which fetches those pages and
#      rewrites the file URLs in them.
#   3. Neither reachable: export nothing and let the retry settings carry the job.
#
# Fail-open is load-bearing, and it is why the probe hits the mirror rather than
# only the local process: a healthy proxy in front of an unreachable mirror answers
# every request with a 502, which is strictly worse than no proxy at all.

_rayci_pypi_index_setup() {
  # Off CI the mirror is unreachable by design, so leave developer machines alone.
  [[ -n "${BUILDKITE:-}" ]] || return 0
  # /etc/profile can be sourced more than once per job; never probe or start twice.
  [[ -z "${RAYCI_PYPI_INDEX_MODE:-}" ]] || return 0

  # Ray CI's own mirror, in the same AWS account as the Buildkite fleets, with one
  # deployment per stack VPC behind a split-horizon name -- so this resolves inside
  # any stack to that stack's instance. Overridable for a fleet that runs its own.
  local mirror="${RAYCI_PYPI_MIRROR_URL:-https://mirror.ci.ray.io}"
  local probe_pkg="pip"

  # Diagnostics first: the resolved address and the reachability verdict are the
  # only way to tell "the mirror is not deployed for this fleet" apart from "the
  # mirror is deployed and this fleet has no route to it".
  local resolved
  resolved="$(getent hosts "${mirror#https://}" 2>/dev/null | awk '{print $1}' | paste -sd, -)"
  echo "pypi index: mirror=${mirror} resolves_to=${resolved:-<unresolved>}"

  local body
  # 1. Rewriting simple index. Accepted only if the page points at the mirror for
  #    artifacts; a page that still names files.pythonhosted.org would leave the
  #    downloads on the origin.
  if body="$(curl -sf -m 15 -H 'Accept: text/html' "${mirror}/simple/${probe_pkg}/" 2>/dev/null)" \
    && [[ -n "${body}" ]]; then
    if grep -qF "${mirror}/files.pythonhosted.org/" <<<"${body}"; then
      export RAYCI_PYPI_INDEX_MODE="mirror-simple"
      export PIP_INDEX_URL="${mirror}/simple"
      export UV_INDEX_URL="${mirror}/simple"
      export RULES_PYTHON_PIP_ISOLATED=0
      echo "pypi index: using the mirror's rewriting simple index -> ${PIP_INDEX_URL}"
      return 0
    fi
    echo "pypi index: ${mirror}/simple answers but does not rewrite artifact URLs; trying the byte cache" >&2
  fi

  # 2. Path-prefixed byte cache, which needs the local rewriting proxy in front.
  if ! curl -sf -m 15 -o /dev/null "${mirror}/pypi.org/simple/${probe_pkg}/" 2>/dev/null; then
    export RAYCI_PYPI_INDEX_MODE="pypi"
    echo "pypi index: mirror unreachable from this agent; resolving from public PyPI" >&2
    return 0
  fi
  local prefix="${RAYCI_PYPI_PROXY_PREFIX:-/opt/pypiproxy}"
  if [[ ! -x "${prefix}/bin/python" ]]; then
    export RAYCI_PYPI_INDEX_MODE="pypi"
    echo "pypi index: byte cache reachable but this image carries no proxy; resolving from public PyPI" >&2
    return 0
  fi

  local port="${RAYCI_PYPI_PROXY_PORT:-35999}"
  local log=/tmp/pypi_index_proxy.log

  # Addressed by this container's bridge address rather than 127.0.0.1, because the
  # nested containers ci/ray_ci starts for tests have their own loopback. The
  # alternative, joining this container's network namespace with
  # `docker run --network container:<id>`, is what build 72099 died on: docker
  # rejects that combination with the `--add-host rayci.localhost:host-gateway` that
  # ci/ray_ci/linux_container.py always passes -- "conflicting options: custom
  # host-to-IP mapping and the network mode" -- so every test container failed to
  # start. Sharing a namespace would also share ports with the tests, which is its
  # own hazard. This address works unchanged from here and from any container on the
  # same bridge.
  # RAYCI_PYPI_PROXY_HOST lets a caller name the address instead. Steps that run
  # directly on an agent rather than in a container -- macOS, and the Windows host --
  # set it, because `hostname -i` is a Linux-only spelling and because there is no
  # nested container that needs to reach this: loopback is both correct and, for pip
  # and uv, exempt from the plain-HTTP refusal below.
  local host="${RAYCI_PYPI_PROXY_HOST:-}"
  if [[ -z "${host}" ]]; then
    host="$(hostname -i 2>/dev/null | awk '{print $1}')"
  fi
  if [[ -z "${host}" ]]; then
    export RAYCI_PYPI_INDEX_MODE="pypi"
    echo "pypi index: could not determine this container's address; resolving from public PyPI" >&2
    return 0
  fi
  local url="http://${host}:${port}"

  # The proxy needs its own session: `bash -i` enables job control, so a plain
  # background job shares the step shell's process group and would be signalled along
  # with it. setsid is util-linux and absent on macOS, where nohup plus a subshell
  # achieves the same detachment.
  if command -v setsid >/dev/null 2>&1; then
    MIRROR_URL="${mirror}" setsid "${prefix}/bin/python" \
      "${prefix}/pypi_index_proxy.py" "${port}" >"${log}" 2>&1 &
  else
    ( MIRROR_URL="${mirror}" nohup "${prefix}/bin/python" \
        "${prefix}/pypi_index_proxy.py" "${port}" >"${log}" 2>&1 & )
  fi

  local _
  for _ in $(seq 1 60); do
    curl -sf -m 5 -o /dev/null "${url}/healthz" 2>/dev/null && break
    sleep 0.5
  done
  # Probed through the proxy rather than at /healthz, which does not touch the
  # mirror: a process that is up but cannot reach its upstream must not be used.
  if ! curl -sf -m 20 -o /dev/null "${url}/simple/${probe_pkg}/" 2>/dev/null; then
    export RAYCI_PYPI_INDEX_MODE="pypi"
    echo "pypi index: proxy did not serve an index in 30s; resolving from public PyPI" >&2
    [[ -f "${log}" ]] && tail -n 40 "${log}" >&2
    return 0
  fi

  export RAYCI_PYPI_INDEX_MODE="local-proxy"
  export RAYCI_PYPI_PROXY_PORT="${port}"
  export RAYCI_PYPI_PROXY_LOG="${log}"
  export PIP_INDEX_URL="${url}/simple"
  export UV_INDEX_URL="${url}/simple"
  # pip and uv treat a plain-HTTP index as insecure and refuse it, with loopback the
  # one exemption -- and this address is deliberately not loopback, so the exemption
  # no longer applies and the host has to be named explicitly. Both variables are
  # already forwarded into nested containers by ci/ray_ci/container.py.
  export PIP_TRUSTED_HOST="${host}"
  export UV_INSECURE_HOST="${host}"
  # rules_python passes --isolated to whl_library's pip, which makes it ignore
  # every PIP_* variable. It reads this before deciding to pass the flag.
  export RULES_PYTHON_PIP_ISOLATED=0

  echo "pypi index: using the local rewriting proxy over the mirror -> ${PIP_INDEX_URL}"
}

_rayci_pypi_index_setup

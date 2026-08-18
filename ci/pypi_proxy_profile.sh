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

  local mirror="${RAYCI_PYPI_MIRROR_URL:-https://mirror.ci.anyscale-test.com}"
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
  if [[ ! -x /opt/pypiproxy/bin/python ]]; then
    export RAYCI_PYPI_INDEX_MODE="pypi"
    echo "pypi index: byte cache reachable but this image carries no proxy; resolving from public PyPI" >&2
    return 0
  fi

  local port="${RAYCI_PYPI_PROXY_PORT:-35999}"
  local url="http://127.0.0.1:${port}"
  local log=/tmp/pypi_index_proxy.log

  # setsid gives the proxy its own session: `bash -i` enables job control, so a
  # plain background job shares the step shell's process group and would be
  # signalled along with it.
  MIRROR_URL="${mirror}" setsid /opt/pypiproxy/bin/python \
    /opt/pypiproxy/pypi_index_proxy.py "${port}" >"${log}" 2>&1 &

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
  # rules_python passes --isolated to whl_library's pip, which makes it ignore
  # every PIP_* variable. It reads this before deciding to pass the flag.
  export RULES_PYTHON_PIP_ISOLATED=0

  # The proxy is on this container's loopback, which a nested container does not
  # share. Publish this container's id so ci/ray_ci/tester.py can join its network
  # namespace; docker sets the hostname to the container's short id.
  local container_id
  container_id="$(cat /etc/hostname 2>/dev/null || hostname)"
  export RAYCI_PYPI_PROXY_NETWORK="container:${container_id}"

  echo "pypi index: using the local rewriting proxy over the mirror -> ${PIP_INDEX_URL}"
}

_rayci_pypi_index_setup

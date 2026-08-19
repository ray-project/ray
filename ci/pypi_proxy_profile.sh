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
  # Bazel's own downloader is a separate problem from pip's index, and needs a separate
  # answer. rules_python declares its bootstrap wheels as http_archive with literal
  # files.pythonhosted.org URLs, and http_archive never consults PIP_INDEX_URL -- so no
  # index setting reaches them. That is what failed microcheck 52470 and the docs-example
  # jobs on postmerge 19272: bazel's Java downloader taking a 502 on click-8.0.1, during
  # repository mapping, which takes the whole analysis phase with it.
  #
  # --experimental_downloader_config rewrites download URLs before they are fetched, so
  # those archives come from the mirror instead. It needs only the mirror, not the local
  # proxy, which is why it is set up here rather than after the proxy starts: an image
  # with no proxy still gets its bazel downloads mirrored.
  #
  # Written per job rather than committed to .bazelrc because the hostname is
  # VPC-internal: a checkout outside CI would rewrite its downloads to a name that does
  # not resolve, turning a working build into a broken one.
  _rayci_bazel_downloader_config() {
    local cfg="${TMPDIR:-/tmp}/rayci_bazel_downloader.cfg"
    local rc="${HOME}/.bazelrc"
    local host="${mirror#*://}"

    # Bazel preserves the original scheme when it rewrites, and every URL rewritten here
    # is https. A mirror reachable only over http would therefore be addressed as https
    # and fail in a way that looks like a broken mirror rather than a misconfiguration,
    # so leave bazel alone in that case.
    if [[ "${mirror}" != https://* ]]; then
      echo "pypi index: mirror is not https, leaving bazel downloads on the origin" >&2
      return 0
    fi

    # The replacement keeps the upstream host as a path segment, which is how the mirror
    # addresses upstreams, and bazel preserves the original https scheme.
    echo "rewrite files\\.pythonhosted\\.org/(.*) ${host}/files.pythonhosted.org/\$1" >"${cfg}" || return 0

    # Appended once: this file may already carry the bazel cache settings the image
    # wrote, and profile.d can be sourced more than once per job.
    if ! grep -qsF -- "--experimental_downloader_config=${cfg}" "${rc}"; then
      echo "common --experimental_downloader_config=${cfg}" >>"${rc}" || return 0
    fi
    echo "pypi index: bazel downloads of files.pythonhosted.org rewritten to ${host}"
  }
  _rayci_bazel_downloader_config

  if [[ ! -x /opt/pypiproxy/bin/python ]]; then
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
  local host
  host="$(hostname -i 2>/dev/null | awk '{print $1}')"
  if [[ -z "${host}" ]]; then
    export RAYCI_PYPI_INDEX_MODE="pypi"
    echo "pypi index: could not determine this container's address; resolving from public PyPI" >&2
    return 0
  fi
  local url="http://${host}:${port}"

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

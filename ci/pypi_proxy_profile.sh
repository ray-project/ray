# shellcheck shell=bash
# Installed as /etc/profile.d/zz-rayci-pypi-proxy.sh in the forge image.
#
# Points pip, uv and bazel at the CI package mirror when this job can reach it, and
# leaves them on public PyPI when it cannot. Steps run under `bash -elic`, a login
# shell, so this is sourced automatically and every step gets it without per-step
# wiring. The variables it exports are the ones .bazelrc forwards to repository
# rules and ci/ray_ci/container.py forwards into nested containers.
#
# Which index to use is decided by probing, not by assuming. The mirror is a byte
# cache and not an index -- it parses only /<host>/<path>, and serves cache hits as
# 303 redirects to presigned S3 URLs, so it never holds a body it could rewrite --
# so there is no mode where pip points straight at it, and two outcomes:
#
#   1. Mirror reachable and this image carries the proxy: start
#      ci/pypi_index_proxy.py, which reads PyPI's index pages through the mirror and
#      rewrites the file URLs in them, and point pip and uv at that.
#   2. Mirror unreachable, or reachable but no proxy in this image: export nothing
#      and let the retry settings carry the job.
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

  # The mirror is a byte cache, not an index. It parses only /<host>/<path>, so
  # <mirror>/simple/ reads as a host named "simple"; and it serves cache hits as 303
  # redirects to presigned S3 URLs, so it never holds a body it could rewrite. There is
  # therefore no mode where pip points straight at it -- an index has to be put in front.
  # This probe establishes reachability for everything below, including the bazel
  # downloader rewrite, which needs only the mirror and not the proxy.
  if ! curl -sf -m 15 -o /dev/null "${mirror}/pypi.org/simple/${probe_pkg}/" 2>/dev/null; then
    export RAYCI_PYPI_INDEX_MODE="pypi"
    echo "pypi index: mirror unreachable from this agent; resolving from public PyPI" >&2
    return 0
  fi
  # Bazel's own downloader is a separate problem from pip's index and gets its own
  # answer, in ci/bazel_mirror_downloader.sh. Called here rather than after the proxy
  # starts because it needs only the mirror: an image carrying no proxy still gets its
  # bazel downloads mirrored.
  #
  # Found rather than assumed, because this file is installed into the image as
  # /etc/profile.d/zz-rayci-pypi-proxy.sh and runs at shell start, when the only copy it
  # can count on is the one install_pypi_proxy.sh put beside the proxy. The checkout
  # paths cover images that carry no proxy.
  _rayci_source_bazel_downloader() {
    local candidate
    for candidate in \
      "${RAYCI_BAZEL_DOWNLOADER_LIB:-}" \
      "${RAYCI_PYPI_PROXY_PREFIX:-/opt/pypiproxy}/bazel_mirror_downloader.sh" \
      "${RAYCI_CHECKOUT_DIR:+${RAYCI_CHECKOUT_DIR}/ci/bazel_mirror_downloader.sh}" \
      "./ci/bazel_mirror_downloader.sh"; do
      [[ -n "${candidate}" && -f "${candidate}" ]] || continue
      # shellcheck source=ci/bazel_mirror_downloader.sh
      source "${candidate}" && return 0
    done
    return 1
  }
  if _rayci_source_bazel_downloader; then
    rayci_bazel_downloader_config "${mirror}"
  else
    echo "pypi index: no bazel downloader helper found; bazel downloads stay on the origin" >&2
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

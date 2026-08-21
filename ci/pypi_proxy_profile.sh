# shellcheck shell=bash
# Installed as /etc/profile.d/zz-rayci-pypi-proxy.sh in the forge and manylinux
# images, sourced by .buildkite/hooks/pre-command for steps that run directly on
# the agent, and by ci/ray_ci/macos/pypi_proxy.sh on the macOS fleet. One file
# makes the decision for every surface.
#
# Points pip, uv and bazel at the CI package mirror when this job can reach it, and
# leaves them on public PyPI when it cannot. Steps in the images run under
# `bash -elic`, a login shell, so this is sourced automatically and every step gets
# it without per-step wiring. The variables it exports are the ones .bazelrc
# forwards to repository rules and ci/ray_ci/container.py forwards into nested
# containers.
#
# The mirror fleet hosts a PyPI index at <mirror>/_pypi/simple: an index proxy on
# the mirror instances serves PyPI's simple-index pages with every
# files.pythonhosted.org URL rewritten to the mirror, so resolution and artifact
# downloads both stay on the cache (pypi/support#11895), and the sha256 hashes in
# the pages pass through the rewrite untouched. Because it is one HTTPS URL on a
# public CA, resolvable anywhere the mirror's split-horizon name resolves, the same
# value serves every consumer -- pip and uv on the agent, docker builds, and the
# nested containers ci/ray_ci starts -- with no local process, no port, and no
# trusted-host handling.
#
# Which index to use is decided by probing, not by assuming. Fail-open is
# load-bearing: a package mirror must never be the reason a job fails. Every
# failure path leaves the environment untouched, and a step that gets nothing
# exported resolves from public PyPI exactly as it does today.

_rayci_pypi_index_setup() {
  # Off CI the mirror is unreachable by design, so leave developer machines alone.
  [[ -n "${BUILDKITE:-}" ]] || return 0
  # /etc/profile can be sourced more than once per job; never probe twice.
  [[ -z "${RAYCI_PYPI_INDEX_MODE:-}" ]] || return 0

  # Ray CI's own mirror, in the same AWS account as the Buildkite fleets, with one
  # deployment per stack VPC behind a split-horizon name -- so this resolves inside
  # any stack to that stack's instance. Overridable for a fleet that runs its own.
  local mirror="${RAYCI_PYPI_MIRROR_URL:-https://mirror.ci.ray.io}"
  local index="${mirror}/_pypi/simple"
  local probe_pkg="pip"

  # Diagnostics first: the resolved address and the reachability verdict are the
  # only way to tell "the mirror is not deployed for this fleet" apart from "the
  # mirror is deployed and this fleet has no route to it". getent is Linux-only;
  # on macOS the address line is simply absent.
  local resolved
  resolved="$(getent hosts "${mirror#https://}" 2>/dev/null | awk '{print $1}' | paste -sd, -)"
  echo "pypi index: mirror=${mirror} resolves_to=${resolved:-<unresolved>}"

  # Bazel's own downloader is a separate mechanism from pip's index and gets its
  # own answer, in ci/bazel_mirror_downloader.sh: rules_python declares its
  # bootstrap wheels as http_archive with literal files.pythonhosted.org URLs,
  # which no index setting reaches.
  #
  # Found rather than assumed, because this file is installed into the image as
  # /etc/profile.d/zz-rayci-pypi-proxy.sh and runs at shell start, when the only
  # copy it can count on is the one the image build put in /etc/rayci. The
  # BASH_SOURCE and checkout paths cover the agent hook and macOS, which source
  # this from a checkout.
  _rayci_source_bazel_downloader() {
    local candidate
    for candidate in \
      "${RAYCI_BAZEL_DOWNLOADER_LIB:-}" \
      "$(dirname "${BASH_SOURCE[0]}")/bazel_mirror_downloader.sh" \
      "/etc/rayci/bazel_mirror_downloader.sh" \
      "${RAYCI_CHECKOUT_DIR:+${RAYCI_CHECKOUT_DIR}/ci/bazel_mirror_downloader.sh}" \
      "./ci/bazel_mirror_downloader.sh"; do
      [[ -n "${candidate}" && -f "${candidate}" ]] || continue
      # shellcheck source=ci/bazel_mirror_downloader.sh
      source "${candidate}" && return 0
    done
    return 1
  }
  _rayci_bazel_mirror_config() {
    if _rayci_source_bazel_downloader; then
      rayci_bazel_downloader_config "${mirror}"
    else
      echo "pypi index: no bazel downloader helper found; bazel downloads stay on the origin" >&2
    fi
  }

  if ! curl -sf -m 15 -o /dev/null "${index}/${probe_pkg}/" 2>/dev/null; then
    export RAYCI_PYPI_INDEX_MODE="pypi"
    echo "pypi index: mirror index unreachable from this agent; resolving from public PyPI" >&2
    return 0
  fi

  # A reachable index implies a reachable mirror: the fleet's index serves nothing
  # its local mirror cannot fetch, which is all the bazel rewrite needs.
  _rayci_bazel_mirror_config

  export RAYCI_PYPI_INDEX_MODE="hosted-index"
  export PIP_INDEX_URL="${index}"
  export UV_INDEX_URL="${index}"
  # rules_python passes --isolated to whl_library's pip, which makes it ignore
  # every PIP_* variable. It reads this before deciding to pass the flag.
  export RULES_PYTHON_PIP_ISOLATED=0
  # The index for docker builds: wanda resolves build args from its own process
  # environment on the agent, and inside images ci/ray_ci/linux_container.py reads
  # this and forwards it as a --build-arg to the docker builds it starts. The same
  # URL works everywhere -- it is not loopback, so no --add-host mapping is
  # involved, and HTTPS on a public CA means no trusted-host handling.
  export RAYCI_IMAGE_PIP_INDEX_URL="${index}"

  echo "pypi index: using the mirror-hosted index -> ${PIP_INDEX_URL}"
}

_rayci_pypi_index_setup

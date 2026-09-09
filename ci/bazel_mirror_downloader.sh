# shellcheck shell=bash
# Point bazel's own downloader at the CI package mirror. Source this, then call
# rayci_bazel_downloader_config where the mirror has just been found reachable, or
# rayci_bazel_downloader_restore where ~/.bazelrc has just been rewritten.
#
# This is a separate mechanism from the pip index, and needs a separate answer:
# rules_python declares its bootstrap wheels as http_archive with literal
# files.pythonhosted.org URLs, and http_archive never consults PIP_INDEX_URL. A 502 there
# takes the whole analysis phase rather than one target, during repository mapping --
# microcheck 52470 and the docs-example jobs on postmerge 19272, both on click-8.0.1.
#
# Sourced from three places, because bazel runs in all three and each can be the last
# writer of ~/.bazelrc:
#
#   ci/pypi_proxy_profile.sh  the images, once per step, after its mirror probe
#   ci/pypi_proxy_agent.sh    the agent, for the steps that never enter a container --
#                             the release pipeline's init step and wanda image builds
#   ci/env/install-bazel.sh   restore only, because it clears ~/.bazelrc in CI and would
#                             otherwise drop whichever hook ran first
#
# Everything fails open. A package mirror must never be the reason a build fails, and the
# two rewrite rules below leave the origin in place besides.

_rayci_bazel_downloader_cfg_path() {
  echo "${HOME}/.rayci_bazel_downloader.cfg"
}

# Add the option to ~/.bazelrc unless it is already there. profile.d can be sourced more
# than once per job, and the file may already carry the bazel cache settings the image
# wrote, so this appends rather than replaces.
_rayci_bazel_downloader_add_rc() {
  local cfg="$1"
  local rc="${HOME}/.bazelrc"

  if ! grep -qsF -- "--experimental_downloader_config=${cfg}" "${rc}"; then
    # Leading newline: appending to a file that does not end in one would splice this
    # onto the last option rather than adding it.
    printf '\n%s\n' "common --experimental_downloader_config=${cfg}" >>"${rc}" || return 0
  fi
}

# Write the rewrite config and reference it from ~/.bazelrc. Call this only where the
# mirror has been probed and answered: the config file's existence is what
# rayci_bazel_downloader_restore later reads as "this job has a reachable mirror".
rayci_bazel_downloader_config() {
  local mirror="$1"
  local cfg
  cfg="$(_rayci_bazel_downloader_cfg_path)"
  local host="${mirror#*://}"

  # Off CI the mirror is unreachable by design, so leave developer machines alone.
  [[ -n "${BUILDKITE:-}" ]] || return 0

  # Bazel preserves the original scheme when it rewrites, and every URL rewritten here is
  # https. A mirror reachable only over http would therefore be addressed as https and
  # fail in a way that looks like a broken mirror rather than a misconfiguration.
  if [[ "${mirror}" != https://* ]]; then
    echo "pypi index: mirror is not https, leaving bazel downloads on the origin" >&2
    return 0
  fi

  # The replacement keeps the upstream host as a path segment, which is how the mirror
  # addresses upstreams.
  # Two rules, and the second is load-bearing. A matching rewrite *replaces* the URL set
  # rather than adding to it, so the first rule alone would discard the origin -- turning
  # a mirror outage into a hard failure where bazel would otherwise have fallen back.
  # Verified against bazel 7.5.0: with an unreachable mirror, one rule fails the build and
  # two rules warn and succeed. Credit to anyscale/rayturbo#4205, which found this and
  # notes it also discards the fallbacks auto_http_archive builds for the C++
  # dependencies if the pattern is ever broadened beyond pythonhosted.
  {
    echo "rewrite files\\.pythonhosted\\.org/(.*) ${host}/files.pythonhosted.org/\$1"
    echo "rewrite files\\.pythonhosted\\.org/(.*) files.pythonhosted.org/\$1"
  } >"${cfg}" || return 0

  _rayci_bazel_downloader_add_rc "${cfg}"
  echo "pypi index: bazel downloads of files.pythonhosted.org rewritten to ${host}"
}

# Re-add the option after something has rewritten ~/.bazelrc. Deliberately does not probe
# or write the config: it restores what was clobbered and nothing more, so a job whose
# mirror was unreachable stays on the origin rather than acquiring a rewrite here.
rayci_bazel_downloader_restore() {
  local cfg
  cfg="$(_rayci_bazel_downloader_cfg_path)"

  [[ -n "${BUILDKITE:-}" ]] || return 0
  [[ -f "${cfg}" ]] || return 0

  _rayci_bazel_downloader_add_rc "${cfg}"
  echo "pypi index: restored the bazel downloader rewrite to ~/.bazelrc"
}

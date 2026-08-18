# shellcheck shell=sh
# Installed as /etc/profile.d/zz-rayci-codeartifact.sh in the CI images.
#
# Steps run their command with `bash -elic`, a login shell, so this is sourced
# automatically and every step picks up the package index with no per-step
# wiring. That matters because the container is started by the Buildkite docker
# plugin, whose --env list is fixed and carries no index configuration: exporting
# these on the agent cannot reach the build, and bazel's --repo_env passthrough
# then has nothing to inherit.
#
# The agent writes both files into the checkout, which is bind-mounted here, so
# $PWD resolves them without either side knowing the other's mount path.
#
# Everything is conditional on the files existing. A plain checkout has neither,
# nothing is exported, and pip and uv resolve from PyPI exactly as before.

if [ -f "${PWD}/.rayci-codeartifact.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${PWD}/.rayci-codeartifact.env"
  set +a

  # The credential is a netrc rather than userinfo in the index URL: it keeps the
  # token out of the environment, and a half-credentialled https://aws@host/ URL
  # would make requests look up a machine named "aws@<host>", miss, and then
  # authenticate with an empty password.
  if [ -f "${PWD}/.rayci-netrc" ]; then
    NETRC="${PWD}/.rayci-netrc"
    export NETRC
  fi
fi

#!/bin/bash

set -ex

export CI="true"
export PYTHON="3.10"
export RAY_BUILD_ENV="macos-py${PYTHON}"
export RAY_USE_RANDOM_PORTS="1"
export RAY_DEFAULT_BUILD="1"
export LC_ALL="en_US.UTF-8"
export LANG="en_US.UTF-8"
export BUILD="1"
export DL="1"
export TORCH_VERSION=2.3.0
export TORCHVISION_VERSION=0.18.0


build() {
  # Cleanup environments
  rm -rf /tmp/bazel_event_logs
  # shellcheck disable=SC2317
  cleanup() { if [[ "${BUILDKITE_PULL_REQUEST}" = "false" ]]; then ./ci/build/upload_build_info.sh; fi }
  trap cleanup EXIT
  (which bazel && bazel clean) || true
  if [[ "$(uname -m)" == "arm64" ]]; then
    brew install pkg-config nvm node || true
  fi
  # Build wheels
  export MAC_WHEELS=1
  export RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1
  # Resolve pip and uv through the CI package mirror where it is reachable, so the
  # installs below do not depend on files.pythonhosted.org being healthy
  # (pypi/support#11895). Sourced, because it exports the index variables. The `|| true`
  # matters under `set -e`: this must never be the reason a wheel build fails, and every
  # path inside it already falls back to public PyPI.
  # shellcheck source=ci/ray_ci/macos/pypi_proxy.sh
  source ./ci/ray_ci/macos/pypi_proxy.sh || true
  . ./ci/ci.sh init && source ~/.zshenv
  source ~/.zshrc
  ./ci/ci.sh build_macos_wheels_and_jars
  # Test wheels
  ./ci/ci.sh test_macos_wheels
  # Upload the wheels
  # We don't want to push on PRs, in fact, the copy_files will fail because unauthenticated.
  if [[ "$BUILDKITE_PULL_REQUEST" != "false" ]]; then exit 0; fi
  # Upload to branch directory.
  bazel run .buildkite:copy_files -- --destination branch_wheels --path "${PWD}/.whl"
  # Upload to latest directory.
  if [[ "$BUILDKITE_BRANCH" = "master" ]]; then bazel run .buildkite:copy_files -- --destination wheels --path "${PWD}/.whl" ; fi
}

build "$@"

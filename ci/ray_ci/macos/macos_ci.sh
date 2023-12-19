#!/bin/bash

set -ex

export CI="true"
export PYTHON="3.8"
export RAY_USE_RANDOM_PORTS="1"
export RAY_DEFAULT_BUILD="1"
export LC_ALL="en_US.UTF-8"
export LANG="en_US.UTF-8"
export BUILD="1"
export DL="1"

run_small_test() {
  # shellcheck disable=SC2046
  bazel test $(./ci/run/bazel_export_options) --config=ci \
    --test_env=CONDA_EXE --test_env=CONDA_PYTHON_EXE --test_env=CONDA_SHLVL --test_env=CONDA_PREFIX \
    --test_env=CONDA_DEFAULT_ENV --test_env=CONDA_PROMPT_MODIFIER --test_env=CI \
    --test_tag_filters=client_tests,small_size_python_tests \
    -- python/ray/tests/...
}

_prelude() {
  rm -rf /tmp/bazel_event_logs
  cleanup() { if [ "${BUILDKITE_PULL_REQUEST}" = "false" ]; then ./ci/build/upload_build_info.sh; fi }; trap cleanup EXIT
  (which bazel && bazel clean) || true;
  . ./ci/ci.sh init && source ~/.zshenv
  source ~/.zshrc
  ./ci/ci.sh build
  ./ci/env/env_info.sh
}

_epilogue() {
  # Persist ray logs
  mkdir -p /tmp/artifacts/.ray/
  tar -czf /tmp/artifacts/.ray/logs.tgz /tmp/ray
  # Cleanup runtime environment to save storage
  rm -rf /tmp/ray
  # Cleanup local caches - this should not clean up global disk cache
  bazel clean
}
trap _epilogue EXIT

_prelude
"$@"

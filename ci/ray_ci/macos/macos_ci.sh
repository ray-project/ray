#!/bin/bash

set -ex

export CI="true"
export PYTHON="3.12"
export RAY_USE_RANDOM_PORTS="1"
export RAY_DEFAULT_BUILD="1"
export LC_ALL="en_US.UTF-8"
export LANG="en_US.UTF-8"
export BUILD="1"
export DL="1"
export RUN_PER_FLAKY_TEST="2"

filter_out_flaky_tests() {
  bazel run ci/ray_ci/automation:filter_tests -- --state_filter=-flaky --prefix=darwin:
}

select_flaky_tests() {
  bazel run ci/ray_ci/automation:filter_tests -- --state_filter=flaky --prefix=darwin:
}

run_tests() {
   # shellcheck disable=SC2046
  bazel test --config=ci $(./ci/run/bazel_export_options) \
      --test_env=CONDA_EXE --test_env=CONDA_PYTHON_EXE --test_env=CONDA_SHLVL --test_env=CONDA_PREFIX \
      --test_env=CONDA_DEFAULT_ENV --test_env=CONDA_PROMPT_MODIFIER --test_env=CI "$@"
}

run_flaky_tests() {
  # shellcheck disable=SC2046
  # 42 is the universal rayci exit code for test failures
  (bazel query 'attr(tags, "client_tests|small_size_python_tests|large_size_python_tests_shard_0|large_size_python_tests_shard_1|large_size_python_tests_shard_2|medium_size_python_tests_a_to_j|medium_size_python_tests_k_to_z", tests(//python/ray/tests/...))' | select_flaky_tests |
    xargs bazel test --config=ci $(./ci/run/bazel_export_options) \
      --runs_per_test="$RUN_PER_FLAKY_TEST" \
      --test_env=CONDA_EXE --test_env=CONDA_PYTHON_EXE --test_env=CONDA_SHLVL --test_env=CONDA_PREFIX \
      --test_env=CONDA_DEFAULT_ENV --test_env=CONDA_PROMPT_MODIFIER --test_env=CI) || exit 42
}

run_small_test() {
  # shellcheck disable=SC2046
  # 42 is the universal rayci exit code for test failures
  (bazel query 'attr(tags, "client_tests|small_size_python_tests", tests(//python/ray/tests/...))' | filter_out_flaky_tests |
    xargs bazel test --config=ci $(./ci/run/bazel_export_options) \
      --test_env=CONDA_EXE --test_env=CONDA_PYTHON_EXE --test_env=CONDA_SHLVL --test_env=CONDA_PREFIX \
      --test_env=CONDA_DEFAULT_ENV --test_env=CONDA_PROMPT_MODIFIER --test_env=CI) || exit 42
}

run_medium_a_j_test() {
  # shellcheck disable=SC2046
  # 42 is the universal rayci exit code for test failures
  (bazel query 'attr(tags, "medium_size_python_tests_a_to_j", tests(//python/ray/tests/...))' | filter_out_flaky_tests |
    xargs bazel test --config=ci $(./ci/run/bazel_export_options) --test_env=CI) || exit 42
}

run_medium_k_z_test() {
  # shellcheck disable=SC2046
  # 42 is the universal rayci exit code for test failures
  (bazel query 'attr(tags, "medium_size_python_tests_k_to_z", tests(//python/ray/tests/...))' | filter_out_flaky_tests |
    xargs bazel test --config=ci $(./ci/run/bazel_export_options) --test_env=CI) || exit 42
}

run_large_test() {
  # shellcheck disable=SC2046
  # 42 is the universal rayci exit code for test failures
  (bazel query 'attr(tags, "large_size_python_tests_shard_'"${BUILDKITE_PARALLEL_JOB}"'", tests(//python/ray/tests/...))' | filter_out_flaky_tests |
    xargs bazel test --config=ci $(./ci/run/bazel_export_options) \
      --test_env=CONDA_EXE --test_env=CONDA_PYTHON_EXE --test_env=CONDA_SHLVL --test_env=CONDA_PREFIX --test_env=CONDA_DEFAULT_ENV \
      --test_env=CONDA_PROMPT_MODIFIER --test_env=CI) || exit 42
}

run_core_dashboard_test() {
  TORCH_VERSION=1.9.0 ./ci/env/install-dependencies.sh
  # Use --dynamic_mode=off until MacOS CI runs on Big Sur or newer. Otherwise there are problems with running tests
  # with dynamic linking.
  # shellcheck disable=SC2046
  # 42 is the universal rayci exit code for test failures
  (bazel test --config=ci --dynamic_mode=off \
    --test_env=CI $(./ci/run/bazel_export_options) --build_tests_only \
    --test_tag_filters=-post_wheel_build -- \
    //:all python/ray/dashboard/... -python/ray/serve/... -rllib/...) || exit 42
}

run_ray_cpp_and_java() {
  # clang-format is needed by java/test.sh
  # 42 is the universal rayci exit code for test failures
  pip install clang-format==12.0.1
  ./java/test.sh || exit 42
  ./ci/ci.sh test_cpp || exit 42
}

bisect() {
  bazel run //ci/ray_ci/bisect:bisect_test -- "$@"
}

_prelude() {
  if [[ "${RAYCI_BISECT_RUN-}" == 1 ]]; then
    echo "RAYCI_BISECT_RUN is set, skipping bazel clean"
  else
    rm -rf /tmp/bazel_event_logs
    (which bazel && bazel clean) || true;
  fi
  . ./ci/ci.sh init && source ~/.zshenv
  source ~/.zshrc
  ./ci/ci.sh build
  ./ci/env/env_info.sh
}

_epilogue() {
  if [[ "${RAYCI_BISECT_RUN-}" == 1 ]]; then
    echo "RAYCI_BISECT_RUN is set, skipping epilogue"
  else
    # Upload test results
    ./ci/build/upload_build_info.sh
    # Assign all macos tests to core for now
    bazel run //ci/ray_ci/automation:test_db_bot -- core /tmp/bazel_event_logs
    # Persist ray logs
    mkdir -p /tmp/artifacts/.ray/
    find /tmp/ray -path '*/logs/*' | tar -czf /tmp/artifacts/.ray/ray_logs.tgz -T -
    # Cleanup runtime environment to save storage
    rm -rf /tmp/ray
    # Cleanup local caches - this should not clean up global disk cache
    bazel clean
  fi
}
trap _epilogue EXIT

_prelude
"$@"

#!/bin/bash -i
# shellcheck disable=SC2046

set -exuo pipefail

PYTHON="3.9"

build() {
  LIB=$1
  conda create -n rllib_contrib python="$PYTHON" -y
  conda activate rllib_contrib
  (cd rllib_contrib/"$LIB" && pip install -r requirements.txt && pip install -e ".[development]")
  ./ci/env/env_info.sh
  # Download files needed for running the bazel tests.
  wget https://raw.githubusercontent.com/ray-project/ray/releases/2.5.1/rllib/tests/run_regression_tests.py -P rllib_contrib/"$LIB"/
}

test_a2c() {
  build "a2c"

  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/a2c/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/a2c/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests,-no_tf_eager_tracing --test_arg=--framework=tf2 rllib_contrib/a2c/...
}

"$@"

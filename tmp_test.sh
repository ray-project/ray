#!/usr/bin/env bash

set -x
bazel test --config=ci $(./ci/run/bazel_export_options) --runs_per_test=1 \
--test_tag_filters=post_wheel_build \
--test_env=CONDA_EXE \
--test_env=CONDA_PYTHON_EXE \
--test_env=CONDA_SHLVL \
--test_env=CONDA_PREFIX \
--test_env=CONDA_DEFAULT_ENV \
--test_env=CI \
--test_env=RAY_CI_POST_WHEEL_TESTS=True \
python/ray/tests/... python/ray/serve/... python/ray/tune/... rllib/... doc/...

cat /tmp/ray/session_*/logs/*
cat /tmp/ray/session_*/logs/serve/*




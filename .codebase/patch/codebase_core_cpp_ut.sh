#!/bin/env/bash
source .codebase/patch/_codebase_prepare.sh
ci/env/install-bazel.sh
export PATH=/root/bin:$PATH
bazel test --config=ci --build_tests_only -- //:all -rllib/... -core_worker_test -ray_syncer_test
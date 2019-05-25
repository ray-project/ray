#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -ex

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR"

python -m pip install pytest-benchmark

pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev0-cp27-cp27mu-manylinux1_x86_64.whl
python -m pytest --benchmark-autosave --benchmark-min-rounds=10 --benchmark-columns="min, max, mean" $ROOT_DIR/../../../python/ray/tests/perf_integration_tests/test_perf_integration.py

pushd $ROOT_DIR/../../../python
python -m pip install -e .
popd

python -m pytest --benchmark-compare --benchmark-min-rounds=10 --benchmark-compare-fail=min:5% --benchmark-columns="min, max, mean" $ROOT_DIR/../../../python/ray/tests/perf_integration_tests/test_perf_integration.py

# This is how Modin stores the values in an S3 bucket
#sha_tag=`git rev-parse --verify --short HEAD`
# save the results to S3
#aws s3 cp .benchmarks/*/*.json s3://modin-jenkins-result/${sha_tag}-perf-${BUCKET_SUFFIX}/ --acl public-read
#rm -rf .benchmarks

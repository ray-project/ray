#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -ex

python -m pip install pytest-benchmark

python -m pytest python/ray/tests/test_perf_integration.py

pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.7.0.dev1-cp27-cp27mu-manylinux1_x86_64.whl
python -m pytest --benchmark-compare python/ray/tests/test_perf_integration.py

# This is how Modin stores the values in an S3 bucket
#sha_tag=`git rev-parse --verify --short HEAD`
# save the results to S3
#aws s3 cp .benchmarks/*/*.json s3://modin-jenkins-result/${sha_tag}-perf-${BUCKET_SUFFIX}/ --acl public-read
#rm -rf .benchmarks

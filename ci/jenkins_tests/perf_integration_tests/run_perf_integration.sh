#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -ex

python -m pip install pytest-benchmark

python -m pytest --benchmark-autosave python/ray/tests/test_perf_integration.py

cp -R .benchmarks current_pr_benchmarks

pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.7.0.dev1-cp27-cp27mu-manylinux1_x86_64.whl
python -m pytest --benchmark-autosave python/ray/tests/test_perf_integration.py

rm -rf .benchmarks
rm -rf current_pr_benchmarks

cat .benchmarks/*/*.json > previous_benchmarks.json
cat current_pr_benchmarks/*/*.json > current_benchmarks.json

rm -rf .benchmarks
rm -rf current_pr_benchmarks

python ci/jenkins_tests/perf_integration_tests/eval_bench_results.py

# This is how Modin stores the values in an S3 bucket
#sha_tag=`git rev-parse --verify --short HEAD`
# save the results to S3
#aws s3 cp .benchmarks/*/*.json s3://modin-jenkins-result/${sha_tag}-perf-${BUCKET_SUFFIX}/ --acl public-read
#rm -rf .benchmarks

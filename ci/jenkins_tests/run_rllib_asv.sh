#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

BUCKET_NAME=ray-integration-testing/ASV
COMMIT=$(cat /ray/git-rev)
RLLIB_RESULTS=RLLIB_RESULTS
RLLIB_RESULTS_DIR=/ray/python/ray/rllib/RLLIB_RESULTS
pip install awscli

# Install Ray fork of ASV
git clone https://github.com/ray-project/asv.git /tmp/asv/ || true
cd /tmp/asv/
pip install -e .

cd /ray/python/ray/rllib/
asv machine --machine jenkins
mkdir $RLLIB_RESULTS_DIR || true
aws s3 cp s3://$BUCKET_NAME/RLLIB_RESULTS/benchmarks.json $RLLIB_RESULTS_DIR/benchmarks.json || true

./tuned_examples/generate_regression_tests.py
asv run --show-stderr --python=same --force-record-commit=$COMMIT

aws s3 cp $RLLIB_RESULTS_DIR/benchmarks.json s3://$BUCKET_NAME/RLLIB_RESULTS/benchmarks_$COMMIT.json
aws s3 sync $RLLIB_RESULTS_DIR/ s3://$BUCKET_NAME/RLLIB_RESULTS/

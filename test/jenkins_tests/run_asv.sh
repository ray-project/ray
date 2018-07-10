#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

BUCKET_NAME=ray-integration-testing/ASV
COMMIT=$(cat /ray/git-rev)
ASV_RESULTS_DIR=/ray/python/ASV_RESULTS
pip install awscli

# Install Ray fork of ASV
git clone https://github.com/ray-project/asv.git /tmp/asv/ || true
cd /tmp/asv/
pip install -e .

cd /ray/python/
asv machine --machine jenkins
mkdir $ASV_RESULTS_DIR || true
aws s3 cp s3://$BUCKET_NAME/ASV_RESULTS/benchmarks.json $ASV_RESULTS_DIR/benchmarks.json || true

asv run --show-stderr --python=same --force-record-commit=$COMMIT

aws s3 cp $ASV_RESULTS_DIR/benchmarks.json s3://$BUCKET_NAME/ASV_RESULTS/benchmarks_$COMMIT.json
aws s3 sync $ASV_RESULTS_DIR/ s3://$BUCKET_NAME/ASV_RESULTS/

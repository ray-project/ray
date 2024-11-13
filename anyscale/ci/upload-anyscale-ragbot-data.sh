#!/bin/bash

set -euo pipefail

bazel run //ci/ray_ci/doc:cmd_build
tar -cvzf /tmp/"${BUILDKITE_COMMIT}".tgz doc/_build/html
pip install awscli==1.35.21
aws s3 cp /tmp/"${BUILDKITE_COMMIT}".tgz s3://anyscale-rag-bot/ray_doc/"${BUILDKITE_COMMIT}".tgz

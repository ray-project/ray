#!/bin/bash

set -euo pipefail

bazel run //ci/ray_ci/doc:cmd_build

# tar the whitelisted and blacklisted contents
tar --exclude="serve/production-guide/*.html" \
  --exclude="serve/develop-and-deploy.html" \
  -cvzf /tmp/"${BUILDKITE_COMMIT}".tgz \
  -C doc/_build/html ray-core data train tune serve rllib ray-observability ray-references

pip install awscli==1.35.21
aws s3 cp /tmp/"${BUILDKITE_COMMIT}".tgz s3://anyscale-rag-bot/ray_doc/"${BUILDKITE_COMMIT}".tgz

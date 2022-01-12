#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -ex

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RAY_DIR=$(cd "${ROOT_DIR}/../../"; pwd)

cd "${RAY_DIR}"

mkdir -p /tmp/bazel_event_logs

./ci/travis/get_build_info.py > /tmp/bazel_event_logs/metadata.json

pip install -q docker aws_requests_auth boto3
python .buildkite/copy_files.py --destination logs --path /tmp/bazel_event_logs

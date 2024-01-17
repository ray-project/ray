#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -ex

BAZEL_LOG_DIR=${1:-"/tmp/bazel_event_logs"}

readonly PIPELINE_POSTMERGE="0189e759-8c96-4302-b6b5-b4274406bf89"
readonly PIPELINE_CIV1_BRANCH="0183465b-c6fb-479b-8577-4cfd743b545d"
if [[
    "${BUILDKITE_PIPELINE_ID:-}" != "${PIPELINE_POSTMERGE}" && 
    "${BUILDKITE_PIPELINE_ID:-}" != "${PIPELINE_CIV1_BRANCH}" 
]]; then
  echo "Skip upload build info. We only upload on postmerge pipelines."
  exit 0
fi

if [[ "${BUILDKITE_BRANCH:-}" != "master" ]]; then
  echo "Skip upload build info. We only upload on master branch."
  exit 0
fi

ROOT_DIR=$(cd "$(dirname "$0")/$(dirname "$(test -L "$0" && readlink "$0" || echo "/")")"; pwd)
RAY_DIR=$(cd "${ROOT_DIR}/../../"; pwd)

cd "${RAY_DIR}"

mkdir -p "$BAZEL_LOG_DIR"

./ci/build/get_build_info.py > "$BAZEL_LOG_DIR"/metadata.json

# Keep cryptography/openssl in sync with `requirements/test-requirements.txt`
pip install -q -c "${RAY_DIR}/python/requirements.txt" docker aws_requests_auth boto3 cryptography==38.0.1 PyOpenSSL==23.0.0
python .buildkite/copy_files.py --destination logs --path "$BAZEL_LOG_DIR"

#!/bin/bash

set -exuo pipefail

readonly PIPELINE_POSTMERGE="0189e759-8c96-4302-b6b5-b4274406bf89"
readonly ARTIFACT_PATH="python/dist"

# Sync the directory to buildkite artifacts
mkdir -p /c/artifact-mount/"$ARTIFACT_PATH"
cp -r "$ARTIFACT_PATH" /c/artifact-mount/"$ARTIFACT_PATH"

if [[ "${BUILDKITE_PIPELINE_ID:-}" != "${PIPELINE_POSTMERGE}" ]]; then
    exit 0
fi

pip install -q docker aws_requests_auth boto3
python .buildkite/copy_files.py --destination branch_wheels --path "$ARTIFACT_PATH"
if [[ "${BUILDKITE_BRANCH}" == "master" ]]; then
    python .buildkite/copy_files.py --destination wheels --path "$ARTIFACT_PATH"
fi

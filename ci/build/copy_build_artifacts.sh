#!/bin/bash

set -exuo pipefail

ARTIFACT_TYPE=${1:-wheel}
if [[ "$OSTYPE" == "msys" ]]; then
  ARTIFACT_MOUNT="/c/artifact-mount"
else
  ARTIFACT_MOUNT="/artifact-mount"
fi

if [[ "$ARTIFACT_TYPE" != "wheel" && "$ARTIFACT_TYPE" != "jar" ]]; then
  echo "Invalid artifact type: $ARTIFACT_TYPE"
  exit 1
fi

if [[ "$ARTIFACT_TYPE" == "wheel" ]]; then
  BRANCH_DESTINATION="branch_wheels"
  MASTER_DESTINATION="wheels"
  if [[ "$OSTYPE" == "msys" ]]; then
    ARTIFACT_PATH="python/dist"
  else
    ARTIFACT_PATH=".whl"
  fi
else
  BRANCH_DESTINATION="branch_jars"
  MASTER_DESTINATION="jars"
  ARTIFACT_PATH=".jar/linux"
fi

if [[ "$OSTYPE" == "msys" ]]; then
  ARTIFACT_PATH="python/dist"
  ARTIFACT_MOUNT="/c/artifact-mount"
fi

export PATH=/opt/python/cp39-cp39/bin:$PATH
pip install -U --ignore-installed -c python/requirements_compiled.txt \
  aws_requests_auth boto3 urllib3 cryptography pyopenssl
./ci/env/env_info.sh

# Sync the directory to buildkite artifacts
ARTIFACT_MOUNT_PATH="$ARTIFACT_MOUNT/$ARTIFACT_PATH"
rm -rf "$ARTIFACT_MOUNT_PATH" || true
mkdir -p "$ARTIFACT_MOUNT_PATH"
cp -r "$ARTIFACT_PATH" "$ARTIFACT_MOUNT_PATH"
chmod -R 777 "$ARTIFACT_MOUNT_PATH"

# Upload to the wheels S3 bucket when running on postmerge pipeline.
readonly PIPELINE_POSTMERGE="0189e759-8c96-4302-b6b5-b4274406bf89"
if [[ "${BUILDKITE_PIPELINE_ID:-}" == "${PIPELINE_POSTMERGE}" ]]; then
  # Upload to branch directory.
  python .buildkite/copy_files.py --destination "$BRANCH_DESTINATION" --path "./$ARTIFACT_PATH"

  # Upload to latest directory.
  if [[ "$BUILDKITE_BRANCH" == "master" ]]; then
    python .buildkite/copy_files.py --destination "$MASTER_DESTINATION" --path "./$ARTIFACT_PATH"
  fi
fi

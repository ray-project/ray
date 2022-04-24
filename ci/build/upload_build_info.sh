#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -ex

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RAY_DIR=$(cd "${ROOT_DIR}/../../"; pwd)

cd "${RAY_DIR}"

# Cleanup old entries, this is needed in macOS shared environment.
if [[ "${OSTYPE}" = darwin* ]]; then
  if [[ -n "${BUILDKITE-}" ]]; then
    echo "Cleanup old entries in macOS"
    rm -rf /tmp/bazel_event_logs
  fi
fi
mkdir -p /tmp/bazel_event_logs

./ci/build/get_build_info.py > /tmp/bazel_event_logs/metadata.json

if [[ -z "${BUILDKITE-}" ]]; then
    # Codepath for Github Actions and Travis CI
    pip install -q awscli

    # Strip the leading "refs/heads/" in the posssible branch tag
    TRAVIS_BRANCH=${TRAVIS_BRANCH/refs\/heads\//}

    export AWS_ACCESS_KEY_ID=AKIAQQPDA73RF7PSLH5N
    export AWS_SECRET_ACCESS_KEY=${BAZEL_LOG_BUCKET_ACCESS_KEY}
    export AWS_DEFAULT_REGION=us-west-2

    DST="s3://ray-travis-logs/bazel_events/$TRAVIS_BRANCH/$TRAVIS_COMMIT/$TRAVIS_JOB_ID"
    echo "Uploading log to ${DST}"

    aws s3 cp --recursive /tmp/bazel_event_logs "${DST}"
else
    if [[ "${OSTYPE}" = darwin* ]]; then
        echo "Using Buildkite Artifact Store on macOS"
    else
        # Codepath for Buildkite
        pip install -q docker aws_requests_auth boto3
        python .buildkite/copy_files.py --destination logs --path /tmp/bazel_event_logs
    fi
fi

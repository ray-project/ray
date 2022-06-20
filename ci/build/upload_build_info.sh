#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -ex

ROOT_DIR=$(cd "$(dirname "$0")/$(dirname "$(test -L "$0" && readlink "$0" || echo "/")")"; pwd)
RAY_DIR=$(cd "${ROOT_DIR}/../../"; pwd)

cd "${RAY_DIR}"

cleanup() {
  # Cleanup the directory because macOS file system is shared between builds.
  rm -rf /tmp/bazel_event_logs
}
trap cleanup EXIT

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
    # Codepath for Buildkite
    pip install -q docker aws_requests_auth boto3
    python .buildkite/copy_files.py --destination logs --path /tmp/bazel_event_logs
fi

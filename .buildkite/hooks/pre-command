#!/bin/bash
# This script is executed by Buildkite on the host machine.
# In contrast, our build jobs are run in Docker containers.
# This means that even though our build jobs write to
# `/artifact-mount`, the directory on the host machine is
# actually `/tmp/artifacts`.
# We clean up the artifacts directory before any command and
# after uploading artifacts to make sure no stale artifacts
# remain on the node when a Buildkite runner is re-used.
set -ex

echo "Creating artifact directory..."
if [[ "${OSTYPE}" == linux* ]]; then
  if [[ -d "/tmp/artifacts" ]]; then
      echo "Cleaning up old artifacts before command."
      echo "Artifact directory contents before cleanup:"
      find /tmp/artifacts -print || true

      if [[ "$(ls -A /tmp/artifacts)" ]]; then
        echo "Directory not empty, cleaning up..."
        # Need to run in docker to avoid permission issues
        docker run --rm -v /tmp/artifacts:/artifact-mount alpine:latest /bin/sh -c 'rm -rf /artifact-mount/*; rm -rf /artifact-mount/.[!.]*' || true
      else
        echo "Directory already empty, no need to clean up."
      fi
  fi

  docker run --rm -v /tmp/artifacts:/artifact-mount alpine:latest /bin/sh -c 'chown -R 2000 /artifact-mount/' || true
elif [[ "${OSTYPE}" == msys ]]; then
  if [[ -d "/c/tmp/artifacts" ]]; then
    rm -rf /c/tmp/artifacts/*
  fi
  mkdir -p /c/tmp/artifacts
fi


RAYCI_CHECKOUT_DIR="$(pwd)"
export RAYCI_CHECKOUT_DIR

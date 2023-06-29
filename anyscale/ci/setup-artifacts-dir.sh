#!/bin/bash

set -euo pipefail

sudo mkdir -p /artifacts
sudo chown -R "${USER}:root" /artifacts

export ARTIFACTS_DIR="/artifacts/${BUILDKITE_BUILD_ID}/${BUILDKITE_JOB_ID}"

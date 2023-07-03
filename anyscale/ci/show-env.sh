#!/bin/bash

set -euo pipefail

buildkite-agent annotate --style "info" --context "build-env" \
    "RUNTIME_BUILD_ID: <code>$RUNTIME_BUILD_ID</code>"

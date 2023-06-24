#!/bin/bash

set -euo pipefail

PIPELINE_YAML="${PIPELINE_YAML:-runtime/ci/buildkite/premerge.yaml}"

buildkite-agent pipeline upload "${PIPELINE_YAML}"

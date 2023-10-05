#!/bin/bash

set -euo pipefail

PIPELINE_YAML="${1:-anyscale/ci/buildkite/premerge.yaml}"

buildkite-agent pipeline upload "${PIPELINE_YAML}"

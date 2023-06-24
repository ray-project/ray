#!/bin/bash

set -euo pipefail

buildkite-agent pipeline upload runtime/ci/buildkite/premerge.yaml

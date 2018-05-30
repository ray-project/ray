#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -eo pipefail

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT"

yapf \
    --style "$ROOT/.style.yapf" \
    --diff --recursive --parallel \
    --exclude 'python/ray/cloudpickle' \
    --exclude 'python/ray/dataframe' \
    --exclude 'python/ray/rllib' \
    -- \
    'test' 'python'


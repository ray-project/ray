#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -eo pipefail

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT"

yapf \
    --style "$ROOT/.style.yapf" \
    --in-place --recursive --parallel \
    --exclude 'python/ray/cloudpickle' \
    --exclude 'python/ray/dataframe' \
    --exclude 'python/ray/rllib' \
    -- \
    'test' 'python'

if ! git diff --quiet; then
    echo 'Reformatted staged files. Please review and stage the changes.'
    echo 'Files updated:'
    echo

    git --no-pager diff --name-only

    exit 1
fi

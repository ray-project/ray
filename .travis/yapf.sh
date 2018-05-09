#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -eo pipefail

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT"

yapf \
    --style "$ROOT/.style.yapf" \
    --in-place --recursive --parallel \
    --exclude 'python/ray/dataframe/' \
    --exclude 'python/ray/rllib/' \
    --exclude 'python/ray/cloudpickle/' \
    -- \
    'test/' 'python/'

CHANGED_FILES=($(git diff --name-only))

if [[ "${#CHANGED_FILES[@]}" -gt 0 ]]; then
    echo 'Reformatted staged files. Please review and stage the changes.'
    echo 'Files updated:'

    for file in "${CHANGED_FILES[@]}"; do
        echo "$file"
    done

    exit 1
fi


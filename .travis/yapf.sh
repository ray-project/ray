#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -eo pipefail

# should be $PROJECT_ROOT
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"
PROJECT_ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$PROJECT_ROOT" && echo "Project Root: $PROJECT_ROOT"

yapf \
    --style "$PROJECT_ROOT/.style.yapf" \
    --in-place --recursive --parallel \
    --exclude 'python/ray/dataframe/' \
    --exclude 'python/ray/rllib/' \
    --exclude 'python/ray/cloudpickle/' \
    -- \
    'test/' 'python/'

CHANGED_FILES=("$(git diff --name-only)")

if [[ "${#CHANGED_FILES[@]}" -gt 0 ]]; then
    echo 'Reformatted staged files. Please review and stage the changes.'
    echo
    echo 'Files updated:'

    for file in "${CHANGED_FILES[@]}"; do
        echo "$file"
    done

    exit 1
else
    exit 0
fi


#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -eo pipefail

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT"

lint() {
    yapf \
        --style "$ROOT/.style.yapf" \
        --in-place --recursive --parallel \
        -- \
        "$@"
}

lint_all() {

    yapf \
        --style "$ROOT/.style.yapf" \
        --in-place --recursive --parallel \
        --exclude 'python/ray/cloudpickle' \
        --exclude 'python/ray/dataframe' \
        --exclude 'python/ray/rllib' \
        -- \
        test python
}

# This flag lints individual files. --files *must* be the first command line
# arg to use this option.
if [[ "$1" == '--files' ]]; then
    lint "${@:2}"
else
    lint_all 'test' 'python'
fi

if ! git diff --quiet; then
    echo 'Reformatted staged files. Please review and stage the changes.'
    echo 'Files updated:'
    echo

    git --no-pager diff --name-only

    exit 1
fi

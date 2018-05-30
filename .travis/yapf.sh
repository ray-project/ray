#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -eo pipefail

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT"

format() {
    yapf \
        --style "$ROOT/.style.yapf" \
        --in-place --recursive --parallel \
        -- \
        "$@"
}

format_changed() {
    # Formats python files that changed in last commit
    git diff --name-only HEAD~1 HEAD | grep '\.py$' | xargs -P 5 \
        yapf \
        --style "$ROOT/.style.yapf" \
        --in-place --recursive --parallel

}

format_all() {

    yapf \
        --style "$ROOT/.style.yapf" \
        --in-place --recursive --parallel \
        --exclude 'python/ray/cloudpickle' \
        --exclude 'python/ray/dataframe' \
        --exclude 'python/ray/rllib' \
        --exclude 'python/build' \
        --exclude 'python/ray/pyarrow_files' \
        --exclude 'python/ray/core/src/ray/gcs' \
        --exclude 'python/ray/common/thirdparty' \
        -- \
        test python
}

# This flag formats individual files. --files *must* be the first command line
# arg to use this option.
if [[ "$1" == '--files' ]]; then
    format "${@:2}"
    # If `--all` is passed, then any further arguments are ignored and the
    # entire python directory is formatted.
elif [[ "$1" == '--all' ]]; then
    format_all 'test' 'python'
else
    # Format only the files that changed in last commit. Ignores uncommitted
    # files.
    format_changed
fi

if ! git diff --quiet; then
    echo 'Reformatted staged files. Please review and stage the changes.'
    echo 'Files updated:'
    echo

    git --no-pager diff --name-only

    exit 1
fi

#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -eo pipefail

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT" || exit 1

YAPF_FLAGS=(
    "--style $ROOT/.style.yapf"
    '--in-place'
    '--recursive'
    '--parallel')

YAPF_EXCLUDES=(
    '--exclude python/ray/dataframe'
    '--exclude python/ray/rllib'
    '--exclude python/ray/cloudpickle'
    '--exclude python/build'
    '--exclude python/ray/pyarrow_files'
    '--exclude python/ray/core/src/ray/gcs'
    '--exclude python/ray/common/thirdparty')

UPSTREAM_MASTER=${RAY_UPSTREAM_BRANCH:-origin/master}

# Format specified files
format() {
    yapf "${YAPF_FLAGS[@]}" -- "$@"
}

# Format files that differ from main branch. Ignores dirs that are not slated
# for autoformat yet.
format_changed() {
    if ! git diff --quiet --exit-code "$UPSTREAM_MASTER" HEAD -- '*.py' &>dev/null; then
        git diff --name-only "$UPSTREAM_MASTER" HEAD -- '*.py' | xargs -P 5 \
            yapf "${YAPF_EXCLUDES[@]}" "${YAPF_FLAGS[@]}"
    fi
}

# Formats *all* files that differ from main branch.
format_all_changed() {
    YAPF_EXCLUDES=() format_changed
}

# Format all files
format_all() {
    yapf "${YAPF_FLAGS[@]}" "${YAPF_EXCLUDES[@]}" python
}

# This flag formats individual files. --files *must* be the first command line
# arg to use this option.
if [[ "$1" == '--files' ]]; then
    format "${@:2}"
    # If `--all` is passed, then any further arguments are ignored and the
    # entire python directory is formatted.
elif [[ "$1" == '--all' ]]; then
    format_all
else
    # Format only the files that changed in last commit.
    format_changed
fi

if ! git diff --quiet &>/dev/null; then
    echo 'Reformatted changed files. Please review and stage the changes.'
    echo 'Files updated:'
    echo

    git --no-pager diff --name-only

    exit 1
fi

#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -eo pipefail

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT" || exit 1

# Add the upstream branch if it doesn't exist
if ! [[ -e "$ROOT/.git/refs/remotes/upstream" ]]; then
    git remote add 'upstream' 'https://github.com/ray-project/ray.git'
fi

# Only fetch master since that's the branch we're diffing against.
git fetch upstream master

YAPF_FLAGS=(
    '--style' "$ROOT/.style.yapf"
    '--recursive'
    '--parallel'
)

YAPF_EXCLUDES=(
    '--exclude' 'python/ray/cloudpickle/*'
    '--exclude' 'python/build/*'
    '--exclude' 'python/ray/pyarrow_files/*'
    '--exclude' 'python/ray/core/src/ray/gcs/*'
    '--exclude' 'python/ray/common/thirdparty/*'
)

# Format specified files
format() {
    yapf --in-place "${YAPF_FLAGS[@]}" -- "$@"
}

# Format files that differ from main branch. Ignores dirs that are not slated
# for autoformat yet.
format_changed() {
    # The `if` guard ensures that the list of filenames is not empty, which
    # could cause yapf to receive 0 positional arguments, making it hang
    # waiting for STDIN.
    #
    # `diff-filter=ACM` and $MERGEBASE is to ensure we only format files that
    # exist on both branches.
    MERGEBASE="$(git merge-base upstream/master HEAD)"

    if ! git diff --diff-filter=ACM --quiet --exit-code "$MERGEBASE" -- '*.py' &>/dev/null; then
        declare -a unformatted_files && mapfile -t unformatted_files < <(git diff --name-only --diff-filter=ACM "$MERGEBASE" -- '*.py')
        yapf --in-place "${YAPF_EXCLUDES[@]}" "${YAPF_FLAGS[@]}" -- "${unformatted_files[@]}"
    fi
}

# Format all files, and print the diff to stdout for travis.
format_all() {
    yapf --diff "${YAPF_FLAGS[@]}" "${YAPF_EXCLUDES[@]}" test python
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

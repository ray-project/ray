#!/bin/sh
set -eu

# This stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "${ROOT}"

# If we change the lint location we should modify the path of lint relative .git
RELATIVE_PATH="../../ci/lint"

ln -sf "${RELATIVE_PATH}/pre-push" "${ROOT}/.git/hooks/pre-push"
ln -sf "${RELATIVE_PATH}/prepare-commit-msg" "${ROOT}/.git/hooks/prepare-commit-msg"


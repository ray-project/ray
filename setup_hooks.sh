#!/bin/bash
set -eu

# This stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "${ROOT}"

GIT_DIR="$(git rev-parse --absolute-git-dir)"
HOOKS_DIR="${GIT_DIR}/hooks"

ln -sf "${ROOT}/ci/lint/pre-push" "${HOOKS_DIR}/pre-push"
ln -sf "${ROOT}/ci/lint/prepare-commit-msg" "${HOOKS_DIR}/prepare-commit-msg"

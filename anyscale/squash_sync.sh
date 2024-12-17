#!/bin/bash

set -euo pipefail

: "${WORK_BRANCH:="$(git branch --show-current)"}"
: "${SYNC_BRANCH:=master}"

: "${UP:=ray}"
: "${UP_GIT:="git@github.com:ray-project/ray.git"}"

: "${DOWN:=rayturbo}"
: "${DOWN_GIT:="git@github.com:anyscale/rayturbo.git"}"

if ! git remote get-url "${UP}" ; then
	git remote add "${UP}" "${UP_GIT}"
fi
ACTUAL_UP_GIT="$(git remote get-url "${UP}")"
if [[ "${ACTUAL_UP_GIT}" != "${UP_GIT}" ]] ; then
	echo "Upstream remote is ${ACTUAL_UP_GIT} , want ${UP_GIT}." > /dev/stderr
	exit 1
fi

if ! git remote get-url "${DOWN}"; then
	git remote add "${DOWN}" "${DOWN_GIT}"
fi
ACTUAL_DOWN_GIT="$(git remote get-url "${DOWN}")"
if [[ "${ACTUAL_DOWN_GIT}" != "${DOWN_GIT}" ]] ; then
	echo "Downstream remote is ${ACTUAL_DOWN_GIT} , want ${DOWN_GIT}." > /dev/stderr
	exit 1
fi


git fetch "${UP}" "${SYNC_BRANCH}"
git fetch "${DOWN}" "${SYNC_BRANCH}"

UP_PARENT="$(git merge-base "${UP}/${SYNC_BRANCH}" "${WORK_BRANCH}")"
DOWN_PARENT="$(git merge-base "${DOWN}/${SYNC_BRANCH}" "${WORK_BRANCH}")"

CURRENT_COMMIT="$(git rev-parse "${WORK_BRANCH}")"
if [[ "${CURRENT_COMMIT}" == "${DOWN_PARENT}" ]] ; then
	echo "Everything is on master; nothing to squash." > /dev/stderr
	exit 0
fi

TREE="$(git show -q --format=%T "${WORK_BRANCH}")"

SQUASHED_COMMIT="$(\
	git commit-tree "${TREE}" \
	-p "${DOWN_PARENT}" \
	-p "${UP_PARENT}" -m "Resolve merge conflict" \
)"

echo "Squashed commit is ${SQUASHED_COMMIT}." > /dev/stderr

if [[ "${DRY_RUN:-}" == "true" ]] ; then
	echo "Dry run, not updating branch." > /dev/stderr
	exit 0
fi

git checkout -B "${WORK_BRANCH}" "${SQUASHED_COMMIT}"

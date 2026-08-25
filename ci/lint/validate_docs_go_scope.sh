#!/usr/bin/env bash
# Scope guard for the "docs-go" pull-request label.
#
# The "docs-go" label skips the per-library doc/example test steps on a pull
# request. (It does not skip the API consistency checks: those are deliberately
# ungated in doc.rayci.yml, because an API reference page edit is exactly the
# content-only change they must still cover.) Skipping is only safe when the PR
# really is documentation content. This guard runs whenever the label is present
# and fails the build unless every changed file is documentation content:
# anything under doc/, the Vale prose-lint configuration at the repo root, or
# the API-consistency checker's own source under ci/ray_ci/doc/ -- in every case
# excluding BUILD files (which define test targets and must not be changed under
# a test-skipping label). It cannot tell an editorial edit from a code edit
# inside a doc file; that judgment stays with the author and is backstopped by
# the post-merge doc build.
#
# Why the Vale configuration counts as documentation content even though it
# lives outside doc/. It defines no bazel target, so nothing the label skips can
# be affected by it, and it holds no executable Ray code, so no doctest or
# example changes behavior because of it. The check that consumes it,
# "lint: documentation_style", carries the `always` tag in lint.rayci.yml, so it
# runs on every pull request whether or not the label is present: widening the
# guard here does not let a Vale edit through unlinted. test.rules.txt already
# routes these paths to `doc` alone, which reaches only the post-merge doc
# build, so no premerge step is traded away either.
#
# Why ci/ray_ci/doc/ counts, on a different argument. This directory is
# executable CI code, so the "no bazel target, no executable code" reasoning
# above does not apply to it. What makes it safe is tag routing. test.rules.txt
# routes ci/ray_ci/doc/ to `doc_api tools` and nothing else, ahead of the broad
# ci/ray_ci/ rule (first match wins). Every step this label skips carries a
# library tag instead -- core_python, data, llm, train, tune, rllib_directly,
# serve, and the *_doc tags -- so a change confined to this directory never
# selects one of them, and the label can only subtract from an already-emitted
# set. Meanwhile the two things that do cover this directory are ungated: the
# `doc_api` API checks, and the `tools` job that runs the six ci_unit py_test
# targets declared here. Editing the checker therefore still runs the checker
# and its own unit tests, with or without the label.
#
# This list is deliberately narrow. For Vale it covers the prose rules
# themselves, not the CI wiring that runs them: ci/lint/check-documentation-style.sh
# and the Vale hook in .pre-commit-config.yaml stay out of scope, because a change
# to either one alters what actually runs. For the checker it covers
# ci/ray_ci/doc/ only, not the shared ci/ray_ci/ tooling that every bazel test
# step runs through.

set -uo pipefail

# Diff against the PR's actual base branch, not a hardcoded master. On a
# release-branch backport the merge-base with master is where the release
# branch diverged, so diffing against master attributes every release-only
# change to the PR and the guard fails a genuinely content-only backport.
# BUILDKITE_PULL_REQUEST_BASE_BRANCH is the base the PR targets; fall back to
# master for local runs, matching ci/lint/lint.sh and
# ci/pipeline/determine_tests_to_run.py.
base_branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-master}"

git fetch --depth=500 origin "${base_branch}" >/dev/null 2>&1 || true
if ! base="$(git merge-base "origin/${base_branch}" HEAD 2>/dev/null)"; then
  echo "docs-go scope guard: could not determine merge-base with origin/${base_branch}; failing closed."
  exit 1
fi

changed="$(git diff --name-only "${base}"...HEAD)"
if [[ -z "${changed}" ]]; then
  echo "docs-go scope guard: no changed files detected; failing closed."
  exit 1
fi

# Paths that count as documentation content: everything under doc/, the Vale
# prose-lint configuration at the repo root, and the API-consistency checker's
# own source.
in_scope_re='^doc/|^\.vale\.ini$|^\.vale/|^ci/ray_ci/doc/'

# Anything outside that set is out of scope for a content-only PR.
out_of_scope="$(printf '%s\n' "${changed}" | grep -vE "${in_scope_re}" || true)"
# BUILD files define test targets, so they are out of scope even in a
# documentation directory.
build_edits="$(printf '%s\n' "${changed}" | grep -E '(^|/)BUILD(\.bazel)?$' | grep -E "${in_scope_re}" || true)"

if [[ -n "${out_of_scope}" || -n "${build_edits}" ]]; then
  echo "The 'docs-go' label is only valid on content-only PRs: changes under doc/, to the Vale configuration (.vale.ini, .vale/), or to the API-consistency checker (ci/ray_ci/doc/), excluding BUILD files."
  echo
  if [[ -n "${out_of_scope}" ]]; then
    echo "Out-of-scope files (not documentation content):"
    printf '%s\n' "${out_of_scope}" | sed 's/^/  /'
  fi
  if [[ -n "${build_edits}" ]]; then
    echo "BUILD files (define test targets; not skippable via docs-go):"
    printf '%s\n' "${build_edits}" | sed 's/^/  /'
  fi
  echo
  echo "Remove the 'docs-go' label so the appropriate tests run, or split the non-doc changes into a separate PR."
  echo
  echo "Removing the label is not enough on its own: push a new commit afterwards."
  echo "A Buildkite rebuild replays the label set from the build it was rebuilt from,"
  echo "and the pipeline skips label-change builds for an already-built commit, so only"
  echo "a new commit produces a build that reads the current labels."
  exit 1
fi

echo "docs-go scope OK: all changed files are documentation content (under doc/, Vale configuration, or ci/ray_ci/doc/, excluding BUILD files)."
printf '%s\n' "${changed}" | sed 's/^/  /'

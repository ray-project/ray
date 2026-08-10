#!/usr/bin/env bash
# Scope guard for the "docs-go" pull-request label.
#
# The "docs-go" label skips the per-team doc/example test steps and the API
# consistency checks on a pull request. That is only safe when the PR really is
# documentation content. This guard runs whenever the label is present and
# fails the build unless every changed file is documentation content: anything
# under doc/, plus the Vale prose-lint configuration at the repo root, and in
# both cases excluding BUILD files (which define test targets and must not be
# changed under a test-skipping label). It cannot tell an editorial edit from a
# code edit inside a doc file; that judgment stays with the author and is
# backstopped by the post-merge doc build.
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
# This list is deliberately narrow. It covers the prose rules themselves, not
# the CI wiring that runs them: ci/lint/check-documentation-style.sh and the
# Vale hook in .pre-commit-config.yaml stay out of scope, because a change to
# either one alters what actually runs.

set -uo pipefail

git fetch --depth=500 origin master >/dev/null 2>&1 || true
if ! base="$(git merge-base origin/master HEAD 2>/dev/null)"; then
  echo "docs-go scope guard: could not determine merge-base with origin/master; failing closed."
  exit 1
fi

changed="$(git diff --name-only "${base}"...HEAD)"
if [[ -z "${changed}" ]]; then
  echo "docs-go scope guard: no changed files detected; failing closed."
  exit 1
fi

# Paths that count as documentation content: everything under doc/, and the
# Vale prose-lint configuration at the repo root.
in_scope_re='^doc/|^\.vale\.ini$|^\.vale/'

# Anything outside that set is out of scope for a content-only PR.
out_of_scope="$(printf '%s\n' "${changed}" | grep -vE "${in_scope_re}" || true)"
# BUILD files define test targets, so they are out of scope even in a
# documentation directory.
build_edits="$(printf '%s\n' "${changed}" | grep -E '(^|/)BUILD(\.bazel)?$' | grep -E "${in_scope_re}" || true)"

if [[ -n "${out_of_scope}" || -n "${build_edits}" ]]; then
  echo "The 'docs-go' label is only valid on content-only PRs: changes under doc/ or to the Vale configuration (.vale.ini, .vale/), excluding BUILD files."
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
  exit 1
fi

echo "docs-go scope OK: all changed files are documentation content (under doc/ or Vale configuration, excluding BUILD files)."
printf '%s\n' "${changed}" | sed 's/^/  /'

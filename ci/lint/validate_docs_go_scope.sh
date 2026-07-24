#!/usr/bin/env bash
# Scope guard for the "docs-go" pull-request label.
#
# The "docs-go" label skips the per-team doc/example test steps and the API
# consistency checks on a pull request. That is only safe when the PR really is
# documentation content. This guard runs whenever the label is present and
# fails the build unless every changed file is documentation content under
# doc/, excluding BUILD files (which define test targets and must not be
# changed under a test-skipping label). It cannot tell an editorial edit from a
# code edit inside a doc file; that judgment stays with the author and is
# backstopped by the post-merge doc build.

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

# Files not under doc/ are out of scope for a content-only PR.
out_of_scope="$(printf '%s\n' "${changed}" | grep -vE '^doc/' || true)"
# BUILD files under doc/ define test targets, so they are out of scope too.
build_edits="$(printf '%s\n' "${changed}" | grep -E '(^|/)BUILD(\.bazel)?$' | grep -E '^doc/' || true)"

if [[ -n "${out_of_scope}" || -n "${build_edits}" ]]; then
  echo "The 'docs-go' label is only valid on content-only PRs: changes under doc/, excluding BUILD files."
  echo
  if [[ -n "${out_of_scope}" ]]; then
    echo "Out-of-scope files (not under doc/):"
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

echo "docs-go scope OK: all changed files are documentation content under doc/ (excluding BUILD files)."
printf '%s\n' "${changed}" | sed 's/^/  /'

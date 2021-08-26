#!/bin/bash

if [ -z "${TRAVIS_PULL_REQUEST-}" ] || [ "${TRAVIS_PULL_REQUEST}" = "false" ]; then
  # Not in a pull request, so compare against parent commit
  base_commit="HEAD^"
  echo "Running clang-tidy against parent commit $(git rev-parse "$base_commit")"
else
  base_commit="$(git merge-base "${TRAVIS_BRANCH}" HEAD)"
  echo "Running clang-tidy against branch $base_commit, with hash $(git rev-parse "$base_commit")"
fi

output="$(git diff -U0 "$base_commit" | ci/travis/clang-tidy-diff.py -p1)"
if [ "$output" = "" ] || [ "$output" = "No relevant changes found." ] ; then
  echo "clang-tidy passed."
  exit 0
else
  echo "clang-tidy failed:"
  echo "$output"
  exit 1
fi

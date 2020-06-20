#!/bin/bash

if [ -z "${TRAVIS_PULL_REQUEST-}" ] || [ "${TRAVIS_PULL_REQUEST}" = "false" ]; then
  # Not in a pull request, so compare against parent commit
  base_commit="HEAD^"
  echo "Running clang-format against parent commit $(git rev-parse "$base_commit")"
else
  base_commit="$(git merge-base "${TRAVIS_BRANCH}" HEAD)"
  echo "Running clang-format against branch $base_commit, with hash $(git rev-parse "$base_commit")"
fi
exclude_regex="(.*thirdparty/|.*redismodule.h|.*.java|.*.jsx?|.*.tsx?)"
output="$(ci/travis/git-clang-format --binary clang-format --commit "$base_commit" --diff --exclude "$exclude_regex")"
if [ "$output" = "no modified files to format" ] || [ "$output" = "clang-format did not modify any files" ] ; then
  echo "clang-format passed."
  exit 0
else
  echo "clang-format failed:"
  echo "$output"
  exit 1
fi

#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" == "false" ] ; then
  # Not in a pull request, so compare against parent commit
  base_commit="HEAD^"
  echo "Running clang-format against parent commit $(git rev-parse $base_commit)"
else
  base_commit="$TRAVIS_BRANCH"
  echo "Running clang-format against branch $base_commit, with hash $(git rev-parse FETCH_HEAD^)"
fi
output="$(.travis/git-clang-format --binary clang-format-3.8 --commit $base_commit --diff --exclude '(.*thirdparty/|.*redismodule.h|.*webui*)')"
if [ "$output" == "no modified files to format" ] || [ "$output" == "clang-format did not modify any files" ] ; then
  echo "clang-format passed."
  exit 0
else
  echo "clang-format failed:"
  echo "$output"
  exit 1
fi

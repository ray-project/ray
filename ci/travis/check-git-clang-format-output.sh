#!/bin/bash

printWarning() {
    printf '\033[31mLINT WARNING (clang-format):\033[0m %s\n' "$@"
}

printInfo() {
    printf '\033[34mLINT (clang-format):\033[0m %s\n' "$@"
}

# Compare against the master branch, because most development is done against it.
base_commit="$(git merge-base HEAD master)"
if [ "$base_commit" = "$(git rev-parse HEAD)" ] && [ "$(git status --porcelain | wc -l)" -eq 0 ]; then
  # Prefix of master branch, so compare against parent commit
  base_commit="$(git rev-parse HEAD^)"
  printInfo "Running clang-format against parent commit $base_commit"
else
  printInfo "Running clang-format against commit $base_commit from master branch"
fi

exclude_regex="(.*thirdparty/|.*redismodule.h|.*.java|.*.jsx?|.*.tsx?)"
output="$(ci/travis/git-clang-format --commit "$base_commit" --diff --exclude "$exclude_regex")"
if [ "$output" = "no modified files to format" ] || [ "$output" = "clang-format did not modify any files" ] ; then
  printInfo "clang-format passed."
  exit 0
else
  printWarning "clang-format failed:"
  printWarning "$output"
  exit 1
fi

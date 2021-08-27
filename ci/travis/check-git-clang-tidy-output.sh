#!/bin/bash

printError() {
    printf '\033[31mERROR:\033[0m %s\n' "$@"
}

printInfo() {
    printf '\033[32mINFO:\033[0m %s\n' "$@"
}

log_err() {
    printError "Running clang-tidy encountered an error"
}

set -x
trap '[ $? -eq 0 ] || log_err' EXIT

printInfo "Fetching workspace info ..."

WORKSPACE=$(bazel info workspace)
BAZEL_ROOT=$(bazel info execution_root)

printInfo "Generating compilation database ..."

CC=clang bazel build --experimental_action_listener=//:compile_command_listener \
    //:extract_compile_command //:ray_pkg

printInfo "Assembling compilation database ..."

TMPFILE=$(mktemp)
printf '[\n' >"$TMPFILE"
find "$BAZEL_ROOT" -name '*.compile_command.json' -exec cat {} + >>"$TMPFILE"
printf '\n]\n' >>"$TMPFILE"

if [[ "${OSTYPE}" =~ darwin* ]]; then
  sed -i '' "s|@BAZEL_ROOT@|$BAZEL_ROOT|g" "$TMPFILE"
  sed -i '' "s/}{/},\n{/g" "$TMPFILE"
else
  sed -i "s|@BAZEL_ROOT@|$BAZEL_ROOT|g" "$TMPFILE"
  sed -i "s/}{/},\n{/g" "$TMPFILE"
fi

OUTFILE=$WORKSPACE/compile_commands.json

if hash jq 2>/dev/null; then
    printInfo "Formatting compilation database ..."
    jq . "$TMPFILE" >"$OUTFILE"
else
    printInfo "Can not find jq. Skip formatting compilation database."
    cp --no-preserve=mode "$TMPFILE" "$OUTFILE"
fi

# Compare against the master branch, because most development is done against it.
base_commit="$(git merge-base HEAD master)"
if [ "$base_commit" = "$(git rev-parse HEAD)" ]; then
  # Prefix of master branch, so compare against parent commit
  base_commit="$(git rev-parse HEAD^)"
  printInfo "Running clang-tidy against parent commit $base_commit"
else
  printInfo "Running clang-tidy against parent commit $base_commit from master branch"
fi

output="$(git diff -U0 "$base_commit" | ci/travis/clang-tidy-diff.py -p1)"
if [ "$output" = "" ] || [ "$output" = "No relevant changes found." ] ; then
  printInfo "clang-tidy passed."
  exit 0
else
  printError "clang-tidy failed:"
  echo "$output"
  exit 1
fi

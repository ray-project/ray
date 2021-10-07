#!/bin/bash

printWarning() {
    printf '\033[31mLINT WARNING (clang-tidy):\033[0m %s\n' "$@"
}

printInfo() {
    printf '\033[34mLINT (clang-tidy):\033[0m %s\n' "$@"
}

log_err() {
    printWarning "Setting up clang-tidy encountered an error"
}

set -eo pipefail

trap '[ $? -eq 0 ] || log_err' EXIT

# Compare against the master branch, because most development is done against it.
base_commit="$(git merge-base HEAD master)"
if [ "$base_commit" = "$(git rev-parse HEAD)" ] && [ "$(git status --porcelain | wc -l)" -eq 0 ]; then
  # Prefix of master branch, so compare against parent commit
  base_commit="$(git rev-parse HEAD^)"
  printInfo "Running clang-tidy against parent commit $base_commit"
else
  printInfo "Running clang-tidy against commit $base_commit from master branch"
fi

WORKSPACE=$(bazel info workspace 2>/dev/null)
BAZEL_ROOT=$(bazel info execution_root 2>/dev/null)

case "${OSTYPE}" in
  linux*)
    printInfo "Generating compile commands with clang (on Linux) ..."
    bazel build //ci/generate_compile_commands:extract_compile_command //:ray_pkg --config=llvm \
        --experimental_action_listener=//ci/generate_compile_commands:compile_command_listener;;
  darwin*)
    printInfo "Generating compile commands with clang (on MacOS) ..."
    bazel build //ci/generate_compile_commands:extract_compile_command //:ray_pkg \
        --experimental_action_listener=//ci/generate_compile_commands:compile_command_listener;;
  msys*)
    printInfo "Generating compile commands with clang (on Windows) ..."
    CC=clang-cl bazel build //ci/generate_compile_commands:extract_compile_command //:ray_pkg \
        --experimental_action_listener=//ci/generate_compile_commands:compile_command_listener;;
esac

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
    jq . "$TMPFILE" >"$OUTFILE"
else
    cp --no-preserve=mode "$TMPFILE" "$OUTFILE"
fi

trap - EXIT

printInfo "Running clang-tidy ..."
output="$(git diff -U0 "$base_commit" | ci/travis/clang-tidy-diff.py -p1 -fix)"
if [[ ! "$output" =~ "error: " ]]; then
  printInfo "clang-tidy passed."
else
  printWarning "clang-tidy issued warnings. See below for details. Suggested fixes have also been applied."
  printWarning "$output"
  printWarning "If a warning is too pedantic, a proposed fix is incorrect or you are unsure about how to fix a warning,"
  printWarning "feel free to raise the issue on the pull request."
  printWarning "clang-tidy warnings can also be suppressed with NOLINT"
fi

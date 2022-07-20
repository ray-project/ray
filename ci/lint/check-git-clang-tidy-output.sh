#!/bin/bash

# TODO: integrate this script into pull request workflow.

printError() {
    printf '\033[31mERROR:\033[0m %s\n' "$@"
}

printInfo() {
    printf '\033[32mINFO:\033[0m %s\n' "$@"
}

log_err() {
    printError "Setting up clang-tidy encountered an error"
}

set -eo pipefail

trap '[ $? -eq 0 ] || log_err' EXIT

printInfo "Fetching workspace info ..."

WORKSPACE=$(bazel info workspace)
BAZEL_ROOT=$(bazel info execution_root)

printInfo "Generating compilation database ..."

case "${OSTYPE}" in
  linux*)
    printInfo "Running on Linux, using clang to build C++ targets. Please make sure it is installed with install-llvm-binaries.sh"
    bazel build //ci/lint/generate_compile_commands:extract_compile_command //:ray_pkg --config=llvm \
        --experimental_action_listener=//ci/lint/generate_compile_commands:compile_command_listener;;
  darwin*)
    printInfo "Running on MacOS, assuming default C++ compiler is clang."
    bazel build //ci/lint/generate_compile_commands:extract_compile_command //:ray_pkg \
        --experimental_action_listener=//ci/lint/generate_compile_commands:compile_command_listener;;
  msys*)
    printInfo "Running on Windows, using clang-cl to build C++ targets. Please make sure it is installed."
    CC=clang-cl bazel build //ci/lint/generate_compile_commands:extract_compile_command //:ray_pkg \
        --experimental_action_listener=//ci/lint/generate_compile_commands:compile_command_listener;;
esac

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

trap - EXIT

if git diff -U0 "$base_commit" | ci/lint/clang-tidy-diff.py -p1 -fix; then
  printInfo "clang-tidy passed."
else
  printError "clang-tidy failed. See above for details including suggested fixes."
  printError
  printError "If you think the warning is too aggressive, the proposed fix is incorrect or are unsure about how to"
  printError "fix, feel free to raise the issue on the PR or Anyscale #learning-cplusplus Slack channel."
  printError
  printError "To run clang-tidy locally with fix suggestions, make sure clang and clang-tidy are installed and"
  printError "available in PATH (version 12 is preferred). Then run"
  printError "ci/lint/check-git-clang-tidy-output.sh"
  printError "from repo root."
fi

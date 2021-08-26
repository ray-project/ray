#!/usr/bin/env sh

# Generates a compile_commands.json file at repo root (bazel info execution_root) for clang tooling needs.

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

bazel build --experimental_action_listener=//:extract_compile_command \
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

# Use `jq` to format the compilation database
if hash jq 2>/dev/null; then
    printInfo "Formatting compilation database ..."
    jq . "$TMPFILE" >"$OUTFILE"
else
    printInfo "Can not find jq. Skip formatting compilation database."
    cp --no-preserve=mode "$TMPFILE" "$OUTFILE"
fi

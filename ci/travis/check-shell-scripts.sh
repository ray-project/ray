#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"

check_shell_scripts_bazel() {
  "${ROOT_DIR}"/bazel.py shellcheck "mnemonic(\"Genrule\", deps(//:*))" "$@"
}

check_shell_scripts_git() {
  local workdir
  workdir="$(git rev-parse --show-cdup)"  # Get the top-level git directory
  # All shell scripts
  {
    # Find shell scripts that have a file extension
    git -C "${workdir}" ls-files --exclude-standard HEAD -- "*.sh"
    # Find shell scripts that don't have a file extension
    git -C "${workdir}" --no-pager grep --cached -l \
      '^#!\(/usr\)\?/bin/\(env \+\)\?\(ba\)\?sh' ":(exclude)*.sh"
  } | {
    local filenames=() filename
    while read -r filename; do filenames+=("${filename}"); done
    "$@" "${filenames[@]}"
  }
}

_check_shell_scripts() {
  local result=0 args=("$@")
  check_shell_scripts_bazel "${args[@]}" || test 0 -ne "${result}" || result="$?"
  check_shell_scripts_git "${args[@]}" || test 0 -ne "${result}" || result="$?"
  return "${result}"
}

check_shell_scripts() {
  local result=0 args=(shellcheck --exclude=1091)
  args+=("$@")
  _check_shell_scripts "${args[@]}" || result="$?"
  if [ 0 -eq "${result}" ]; then
    echo "shellcheck passed."
  elif "${args[@]}" --shell=sh --format=diff - < /dev/null; then  # Are diffs supported?
    echo "shellcheck failed; this patch might fix some issues (but please double-check it):" 1>&2
    _check_shell_scripts "${args[@]}" --format=diff || true   # diffs supported, so enable them
  fi
  return "${result}"
}

check_shell_scripts "$@"

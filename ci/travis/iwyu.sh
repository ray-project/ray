#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"
WORKSPACE_DIR="${ROOT_DIR}/../.."

if [ "${OSTYPE-}" = msys ] && [ -z "${MINGW_DIR+x}" ]; then
  # On Windows MINGW_DIR (analogous to /usr) might not always be defined when we need it for some tools
  if [ "${HOSTTYPE-}" = x86_64 ]; then
    MINGW_DIR=/mingw64
  elif [ "${HOSTTYPE-}" = i686 ]; then
    MINGW_DIR=/mingw32
  fi
fi
invoke_cc() {
  local env_vars=() args=() env_parsed=0 result=0
  if [ "${OSTYPE}" = msys ]; then
    # On Windows we don't want automatic path conversion, since it can break non-path arguments
    env_vars+=("MSYS2_ARG_CONV_EXCL=*")
  fi
  local arg; for arg in "$@"; do
    if [ 0 -ne "${env_parsed}" ]; then
      # Nothing to do if we've already finished converting environment variables.
      args+=("${arg}")
    elif [ "${arg}" == "--" ]; then
      # Reached '--'? Then we're finished parsing environment variables.
      env_parsed=1
    else
      if [ "${OSTYPE}" = msys ] && [ ! "${arg}" = "${arg#PATH=}" ]; then
        # We need to split Windows-style paths and convert them to UNIX-style one-by-one.
        local key="${arg%%=*}"  # name of environment variable
        local value="${arg#*=}"  # value of environment variable
        value="${value//'/'\\''}"  # Escape single quotes
        value="'${value//;/\' \'}'"  # Replace semicolons by spaces for splitting
        local paths; declare -a paths="(${value})"  # Split into array
        value=""
        local path; for path in "${paths[@]}"; do
          if [ "${OSTYPE}" = msys ]; then
            path="${path//\\//}"  # Convert 'C:\...' into 'C:/...'
          fi
          case "${path}" in
            [a-zA-Z]:|[a-zA-Z]:/*)
              # Convert 'C:/...' into '/c/...'
              local drive; drive="${path%%:*}"
              path="/${drive,,}${path#*:}"
              ;;
            *) true;;
          esac
          if [ -n "${value}" ]; then
            value="${value}:"
          fi
          value="${value}${path}"  # Replace with modified PATH
        done
        arg="${key}=${value}"  # Replace the environment variable with the modified version
      fi
      env_vars+=("${arg}")
    fi
  done
  local cc; cc="${args[0]}"
  case "${cc##*/}" in
    clang*)
      # Call iwyu with the modified arguments and environment variables (env -i starts with a blank slate)
      local output
      # shellcheck disable=SC2016
      output="$(PATH="${PATH}:/usr/bin" env -i "${env_vars[@]}" "${SHELL-/bin/bash}" -c 'iwyu -isystem "$("$1" -print-resource-dir "${@:2}")/include" "${@:2}"' exec "${args[@]}" 3>&1 1>&2 2>&3- || true)." || result=$?
      output="${output%.}"
      if [ 0 -eq "${result}" ]; then
        printf "%s" "${output}"
      else
        printf "%s" "${output}" 1>&2
      fi
      ;;
  esac
  return "${result}"
}

_fix_include() {
  "$(command -v python2 || echo python)" "$(command -v fix_include)" "$@"
}

process() {
  # TODO(mehrdadn): Install later versions of iwyu that avoid suggesting <bits/...> headers
  case "${OSTYPE}" in
    linux*)
      sudo apt-get install -qq -o=Dpkg::Use-Pty=0 clang iwyu
      ;;
  esac

  local target
  target="$1"
  CC=clang bazel build -k --config=iwyu "${target}"
  CC=clang bazel aquery -k --config=iwyu --output=textproto "${target}" |
    "$(command -v python3 || echo python)" "${ROOT_DIR}"/bazel.py textproto2json |
    jq -s -r '{
      "artifacts": map(select(.[0] == "artifacts") | (.[1] | map({(.[0]): .[1]}) | add) | {(.id): .exec_path}) | add,
      "outputs": map(select(.[0] == "actions") | (.[1] | map({(.[0]): .[1]}) | add | select(.mnemonic == "iwyu_action") | .output_ids))
    } | (. as $parent | .outputs | map($parent.artifacts[.])) | .[]' |
    sed "s|^|$(bazel info | sed -n "s/execution_root: //p")/|" |
    xargs -r -I {} -- sed -e "/^clang: /d" -e "s|[^ ]*_virtual_includes/[^/]*/||g" {} |
    (
      # Checkout & modify a clean copy of the repo to affecting the current one
      local data new_worktree args=(--nocomments)
      data="$(cat)"
      new_worktree="$(TMPDIR="${WORKSPACE_DIR}/.." mktemp -d)"
      git worktree add -q "${new_worktree}"
      pushd "${new_worktree}"
        echo "${data}" |             _fix_include "${args[@]}"    || true  # HACK: For files are only accessible from the workspace root
        echo "${data}" | { cd src && _fix_include "${args[@]}"; } || true  # For files accessible from src/
        git diff
      popd  # this is required so we can remove the worktree when we're done
      git worktree remove --force "${new_worktree}"
    )
}

postbuild() {
  # Parsing might fail due to various things like aspects (e.g. BazelCcProtoAspect), but we don't want to check generated files anyway
  local data=""  # initialize in case next line fails
  data="$(exec "$1" --decode=blaze.CppCompileInfo "$2" < "${3#*=}" 2>&-)" || true
  if [ -n "${data}" ]; then
    # Convert output to JSON-like format so we can parse it
    data="$(exec sed -e "/^[a-zA-Z]/d" -e "s/^\(\s*\)\([0-9]\+\):\s*\(.*\)/\1[\2, \3],/g" -e "s/^\(\s*\)\([0-9]\+\)\s*{/\1[\2, /g" -e "s/^\(\s*\)}/\1],/g" <<< "${data}")"
    # Turn everything into a single line by replacing newlines with a special character that should never appear in the actual stream
    data="$(exec tr "\n" "\a" <<< "${data}")"
    # Remove some fields that we don't want, and put back newlines we removed
    data="$(exec sed -e "s/\(0x[0-9a-fA-F]*\)]\(,\a\)/\"\1\"]\2/g" -e "s/,\(\a\s*\(]\|\$\)\)/\1/g" -e "s/\a/\n/g" <<< "${data}")"
    # Parse the resulting JSON and select the actual fields we're interested in.
    # We put the environment variables first, separating them from the command-line arguments via '--'.
    # shellcheck disable=SC1003
    data="$(PATH="${PATH}:${MINGW_DIR-/usr}/bin" && exec jq -r '(
        []
        + [.[1:][] | select (.[0] == 6) | "\(.[1][1])=\(.[2][1])" | gsub("'\''"; "'\''\\'\'''\''") | "'\''\(.)'\''"]
        + ["--"]
        + [.[1:][] | select (.[0] == 1) | .[1]                    | gsub("'\''"; "'\''\\'\'''\''") | "'\''\(.)'\''"]
        + [.[1:][] | select (.[0] == 2 and (.[1] | type) == "string") | .[1]                                       ]
      ) | .[]' <<< "${data}")"
    # On Windows, jq can insert carriage returns; remove them
    data="$(exec tr -d "\r" <<< "${data}")"
    # Wrap everything into a single line for passing as argument array
    data="$(exec tr "\n" " " <<< "${data}")"
    eval invoke_cc "${data}"
  fi
}

"$@"

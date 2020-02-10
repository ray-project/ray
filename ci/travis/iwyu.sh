#!/usr/bin/env bash

set -euo pipefail

if [ "${OSTYPE-}" = msys ] && [ -z "${MINGW_DIR+x}" ]; then
  if [ "${HOSTTYPE-}" = x86_64 ]; then
    MINGW_DIR=/mingw64
  elif [ "${HOSTTYPE-}" = i686 ]; then
    MINGW_DIR=/mingw32
  fi
fi
invoke_cc() {
  local env_vars=() args=() env_parsed=0
  if [ "${OSTYPE}" = msys ]; then
    env_vars+=(MSYS2_ARG_CONV_EXCL="*")
  fi
  local arg; for arg in "$@"; do
    if [ 0 -ne "${env_parsed}" ]; then
      args+=("${arg}")
    elif [ "${arg}" == "--" ]; then
      env_parsed=1
    else
      if [ "${OSTYPE}" = msys ] && [ ! "${arg}" = "${arg#PATH=}" ]; then
        local key="${arg%%=*}"
        local value="${arg#*=}"
        value="${value//'/'\\''}"
        value="'${value//;/\' \'}'"
        local paths; declare -a paths="(${value})"
        value=""
        local path; for path in "${paths[@]}"; do
          if [ "${OSTYPE}" = msys ]; then
            path="${path//\\//}"
          fi
          case "${path}" in
            [a-zA-Z]:|[a-zA-Z]:/*)
              local drive; drive="${path%%:*}"
              path="/${drive,,}${path#*:}"
              ;;
            *) true;;
          esac
          if [ -n "${value}" ]; then
            value="${value}:"
          fi
          value="${value}${path}"
        done
        arg="${key}=${value}"
      fi
      env_vars+=("${arg}")
    fi
  done
  local cc; cc="${args[0]}"
  case "${cc##*/}" in
    clang*)
      { PATH="${PATH}:/usr/bin" env -i "${env_vars[@]}" "${SHELL-/bin/bash}" -c 'iwyu -isystem "$("$1" -print-resource-dir "${@:2}")/include" "${@:2}"' exec "${args[@]}" 2>&1 || true; } | awk '
        { header = 0; }
        /^(The full include-list for .*|.* should (add|remove) these lines:)$/ { keep = 1; header = 1; }
        /^(The full include-list for ([^\/]*\/)*external\/.*|([^\/]*\/)*external\/.* should (add|remove) these lines:)$/ { keep = 0; header = 1; }
        /^The full include-list for .*$/ { keep = 0; header = 1; }
        /^---$/ { keep = 0; header = 1; }
        keep { print; n += header ? 0 : 1; }
        END { exit n; }
      '
      ;;
  esac
}

main() {
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
    # Parse the resulting JSON and select the actual fields we're interested in
    data="$(PATH="${PATH}:${MINGW_DIR-/usr}/bin" && exec jq -r '(
        []
        + [.[1:][] | select (.[0] == 6) | "\(.[1][1])=\(.[2][1])" | gsub("'\''"; "'\''\\'\'''\''") | "'\''\(.)'\''"]
        + ["--"]
        + [.[1:][] | select (.[0] == 1) | .[1]                    | gsub("'\''"; "'\''\\'\'''\''") | "'\''\(.)'\''"]
        + [.[1:][] | select (.[0] == 2) | .[1]                                                                     ]
      ) | .[]' <<< "${data}")"
    # On Windows, jq can insert carriage returns; remove them
    data="$(exec tr -d "\r" <<< "${data}")"
    # Wrap everything into a single line for passing as argument array
    data="$(exec tr "\n" " " <<< "${data}")"
    eval invoke_cc "${data}"
  fi
}

main "$@"

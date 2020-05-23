#!/usr/bin/env bash

set -euo pipefail

bazel_query_genrules() {
  bazel aquery --color=no --show_progress=no --output=textproto --include_artifacts=false "$1" | \
  awk '
    {
      body = 0;
    }
    /^ / {
      body = 1;
    }
    /^\S.* {$/ {
      section = $1;
      a = 0;
      delete arguments;
    }
    body {
      if (section == "actions") {
        if ($1 == "arguments:") {
          p = substr($0, index($0, ":") + 2);
          p = substr(p, 2, length(p) - 2);  # strip quotes
          gsub(/\\t/, "\t", p);
          gsub(/\\r/, "\r", p);
          gsub(/\\'\''/, "'\''", p);
          gsub(/\\"/, "\"", p);
          # gsub(/\\n/, "\n", p);  # This should be handled by the client
          # gsub(/\\\\/, "\\", p);  # This should be handled by the client
          arguments[++a] = p;
        }
      }
    }
    /^}/ {
      if (a >= 1) {
        shell = arguments[1];
        gsub(/[\\]/, "/", shell);  # replace backslash (Windows) with slash
        gsub(/\.[^\\/]*$/, "", shell);  # remove suffix (Windows)
        gsub(/^.*[\\/]/, "", shell);
        if (shell == "bash" || shell == "dash" || shell == "ksh" || shell == "sh") {
          next_arg_is_command = 0;
          for (k in arguments) {
            arg = arguments[k];
            if (next_arg_is_command) {
              print("-" "\t" "--shell=" shell "\t" arg);
              break;
            } else if (index(arg, "-") == 1) {
              if (arg == "-c") {
                next_arg_is_command = 1;
              }
            }
          }
        }
      }
    }
  '
}

bazel_query_user_genrules() {
  bazel_query_genrules "$1" | uniq | {
    # Exclude externally-generated rules that we can't change
    grep --line-buffered -v -F \
      -e 'host/bin/external/com_github_google_flatbuffers/flatc' \
      ;
  }
}

process_bazel_query() {
  local result=0 cmd arg body
  cmd="$1"
  command -V "${cmd}" > /dev/null  # ensure command exists before trying it
  while IFS=$'\t' read -r filename arg body; do
    local args=("$@")
    if [ -n "${arg-}" ]; then
      args+=("${arg}")
    fi
      body="${body//\\n/$'\n'}"
      body="${body//\\\\/\\}"
    if [ "${filename}" = "-" ]; then  # Pass via stdin?
      printf "%s" "${body}" | "${args[@]}" "${filename}"
    else
      "${args[@]}" "${filename}" < /dev/null || true
    fi || result="$?"
  done
  return "${result}"
}

check_shell_scripts_bazel() {
  bazel_query_user_genrules "mnemonic(\"Genrule\", deps(//:*))" | process_bazel_query "$@"
}

check_shell_scripts_bazel "$@"

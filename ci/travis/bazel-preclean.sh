#!/usr/bin/env bash

# Args: [Bazel-target]
#
# This script cleans up any genrule() outputs in the transitive dependencies of the provided target.
#
# This is useful for forcing genrule actions to re-run, because the _true_ outputs of those actions
# can include a larger set of files (e.g. files copied to the workspace) which Bazel is unable to
# detect changes to (or delete changes of).
#
# Usually, you would run this script along with 'git clean -f', to make sure Bazel re-copies outputs
# the next time a build occurs.

(
  set -euo pipefail
  bazel aquery --output=textproto \
    "mnemonic(\"Genrule\", deps(${1-//:*}))" | awk '
    {
      body = 0;
    }
    /^^ / {
      body = 1;
    }
    /^^\S.* {$/ {
      section = $1;
      delete arr;
    }
    body {
      if (section == "artifacts") {
        p = $2;
        if ($1 == "exec_path:") {
          p = substr(p, 2, length(p) - 2);  # strip quotes
        }
        arr[$1] = p;
      }
    }
    /^^}/ {
      artifacts[arr["id:"]] = arr["exec_path:"];  # save the ID -> artifact mapping
    }
    /^^ *output_ids:/ {
      print(artifacts[$2]);  # print the output artifact
    }
  ' | tr "\n" "\0" | xargs -0 -r -- rm -f --
)

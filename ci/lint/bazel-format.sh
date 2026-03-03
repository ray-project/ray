#!/usr/bin/env bash

# Before running this script, please make sure golang is installed
# and buildifier is also installed. The example is showed in .travis.yml.
set -eo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/$(dirname "$(test -L "$0" && readlink "$0" || echo "/")")"; pwd)
BUILDIFIER="${BUILDIFIER:-buildifier}"

function usage()
{
  echo "Usage: bazel-format.sh [<args>]"
  echo
  echo "Options:"
  echo "  -h|--help               print the help info"
  echo "  -c|--check              check whether there are format issues in bazel files"
  echo "  -f|--fix                fix all the format issue directly"
  echo
}

RUN_TYPE="diff"

# Parse options
while [ $# -gt 0 ]; do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit 0
      ;;
    -c|--check)
      RUN_TYPE="diff"
      ;;
    -f|--fix)
      RUN_TYPE=fix
      ;;
    *)
      echo "ERROR: unknown option \"$key\""
      echo
      usage
      exit 1
      ;;
  esac
  shift
done

BAZEL_FILES=(
  bazel/BUILD
  bazel/ray.bzl
  BUILD.bazel
  java/BUILD.bazel
  cpp/BUILD.bazel
  cpp/example/_BUILD.bazel
  WORKSPACE
)

(
  cd "$ROOT_DIR"/../..
  "${BUILDIFIER}" -mode=$RUN_TYPE -diff_command="diff -u" "${BAZEL_FILES[@]}"
)

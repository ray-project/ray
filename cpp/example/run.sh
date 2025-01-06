#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

bazel --nosystem_rc --nohome_rc build //cpp/example:example
if [[ "$OSTYPE" == "darwin"* ]]; then
    bazel-bin/cpp/example/example
else
    bazel-bin/cpp/example/example
fi

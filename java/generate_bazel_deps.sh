#!/usr/bin/env bash
# Generate Java dependencies for bazel.

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

jar_file="/tmp/bazel_deps.jar"
if [[ -e $jar_file ]]; then
    echo "Use existing jar file from $jar_file."
else
    echo "Downloading jar file to $jar_file."
    curl -L -o $jar_file "https://github.com/oferb/startupos-binaries/releases/download/0.1.01/bazel_deps.jar"
fi

echo "Generating Java dependencies for bazel."
java -jar $jar_file generate -r $ROOT_DIR/.. -s java/third_party/workspace.bzl -d java/dependencies.yaml

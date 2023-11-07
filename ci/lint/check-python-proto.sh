#!/bin/bash

# Checks that we manually compile protos for python when proto files are changed.

set -euxo pipefail

bazel run  //ci/compile_py_proto:install_py_proto

if ! git diff --quiet &>/dev/null; then
    echo 'Python proto source files are not manually generated when proto files are changed.'
    echo 'Run "bazel run  //ci/compile_py_proto:install_py_proto" to generate.'
    echo

    exit 1
fi

#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RAY_ROOT_DIR="$ROOT_DIR"/../../..
echo "ROOT_DIR $ROOT_DIR"
echo "RAY_ROOT_DIR $RAY_ROOT_DIR"

echo "build all_streaming_tests_deploy.jar"
bazel build //streaming/java:all_streaming_tests_deploy.jar
export CLASSPATH=$RAY_ROOT_DIR/bazel-bin/streaming/java/all_streaming_tests_deploy.jar

python test_hybrid_stream.py
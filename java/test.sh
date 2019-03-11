#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# run this file before compile the targets
sh $ROOT_DIR/generate_deps.sh

pushd $ROOT_DIR/..
bazel build -c opt //java:all

bazel test -c opt //java:all
popd

pushd $ROOT_DIR/../java/test
echo "Running tests under cluster mode."
ENABLE_MULTI_LANGUAGE_TESTS=1 java -jar -Dray.home=$ROOT_DIR/../ $ROOT_DIR/../bazel-bin/java/AllTests_deploy.jar $ROOT_DIR/../java/testng.xml

echo "Running tests under single-process mode."
java -jar -Dray.home=$ROOT_DIR/../ -Dray.run-mode=SINGLE_PROCESS $ROOT_DIR/../bazel-bin/java/AllTests_deploy.jar $ROOT_DIR/../java/testng.xml

popd

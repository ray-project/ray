#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd $ROOT_DIR/../java
echo "Compiling Java code."
mvn clean install -Dmaven.test.skip

echo "Checking code format."
mvn checkstyle:check

echo "Running tests under cluster mode."
ENABLE_MULTI_LANGUAGE_TESTS=1 mvn test

echo "Running tests under single-process mode."
mvn test -Dray.run-mode=SINGLE_PROCESS

set +x
set +e

popd

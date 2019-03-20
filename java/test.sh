#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd $ROOT_DIR/../java
echo "Compiling Java code."
mvn clean install -Dmaven.test.skip

ENABLE_MULTI_LANGUAGE_TESTS=1 mvn test -Dtest="CrossLanguageInvocationTest#*" -pl test
tail -n 100 /tmp/ray/*

set +x

popd

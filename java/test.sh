#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
$ROOT_DIR/../build.sh -l java

pushd $ROOT_DIR/../java
mvn clean install -Dmaven.test.skip
mvn checkstyle:check | tee check_style
[[ ${check_style} =~ "BUILD FAILURE" ]] && exit 1

# test raylet
mvn test | tee mvn_test
[[ ${mvn_test} =~ "BUILD SUCCESS" ]] || exit 1

# test raylet under SINGLE_PROCESS mode
mvn test -Dray.run-mode=SINGLE_PROCESS | tee dev_mvn_test
[[ ${dev_mvn_test} =~ "BUILD SUCCESS" ]] || exit 1
popd

#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
$ROOT_DIR/../build.sh -l java

pushd $ROOT_DIR/../java
mvn clean install -Dmaven.test.skip
check_style=$(mvn checkstyle:check)
echo "${check_style}"
[[ ${check_style} =~ "BUILD FAILURE" ]] && exit 1

# test raylet
mvn_test=$(mvn test)
echo "${mvn_test}"
[[ ${mvn_test} =~ "BUILD SUCCESS" ]] || exit 1

popd

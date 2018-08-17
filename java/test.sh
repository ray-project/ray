#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
$ROOT_DIR/../build.sh -l java,python

pushd $ROOT_DIR/../thirdparty/build/arrow/java
mvn clean install -pl plasma -am -Dmaven.test.skip
popd
pushd $ROOT_DIR/../java
mvn clean install -Dmaven.test.skip
check_style=$(mvn checkstyle:check)
echo "${check_style}"
[[ ${check_style} =~ "BUILD FAILURE" ]] && exit 1

# test non-raylet
sed -i 's/^use_raylet.*$/use_raylet = false/g' $ROOT_DIR/../java/ray.config.ini
mvn_test=$(mvn test)
echo "${mvn_test}"
[[ ${mvn_test} =~ "BUILD SUCCESS" ]] || exit 1

# test raylet
sed -i 's/^use_raylet.*$/use_raylet = true/g' $ROOT_DIR/../java/ray.config.ini
mvn_test=$(mvn test)
echo "${mvn_test}"
[[ ${mvn_test} =~ "BUILD SUCCESS" ]] || exit 1

popd

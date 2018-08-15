#!/bin/bash
set -x

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

RAY_BUILD_PYTHON=$RAY_BUILD_PYTHON \
RAY_BUILD_JAVA=$RAY_BUILD_JAVA \
$ROOT_DIR/thirdparty/scripts/setup.sh $PYTHON_EXECUTABLE

if [[ "$RAY_BUILD_JAVA" == "YES" ]]; then
    pushd $ROOT_DIR/thirdparty/build/arrow/java
    mvn clean install -pl plasma -am -Dmaven.test.skip
    popd
fi

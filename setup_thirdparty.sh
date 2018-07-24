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

LANGUAGE="python"
if [[ -n  "$2" ]]; then
  LANGUAGE=$2
fi

$ROOT_DIR/thirdparty/scripts/setup.sh $PYTHON_EXECUTABLE $LANGUAGE

if [[ "$LANGUAGE" == "java" ]]; then
    pushd $ROOT_DIR/thirdparty/build/arrow/java
    mvn clean install -pl plasma -am -Dmaven.test.skip
    popd
fi

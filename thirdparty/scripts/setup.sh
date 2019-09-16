#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
TP_DIR=$TP_SCRIPT_DIR/..

mkdir -p $TP_DIR/build
mkdir -p $TP_DIR/pkg

if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

if [[ "$RAY_BUILD_JAVA" == "YES" ]]; then
echo "Java library will be built."
fi
if [[ "$RAY_BUILD_PYTHON" == "YES" ]]; then
echo "Python library will be built."
fi

unamestr="$(uname)"

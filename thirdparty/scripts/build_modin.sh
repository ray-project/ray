#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi

PYTHON_VERSION="$($PYTHON_EXECUTABLE -c 'import sys; print(sys.version_info[0])')"

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../
MODIN_VERSION=0.2.4
MODIN_WHEELS="modin-$MODIN_VERSION-py$PYTHON_VERSION-none-any.whl"

pushd $TP_DIR/../python/ray/
rm -rf modin
mkdir modin
pushd modin
$PYTHON_EXECUTABLE -m pip download --no-deps modin==$MODIN_VERSION
unzip $MODIN_WHEELS
rm $MODIN_WHEELS
rm -r "modin-$MODIN_VERSION.dist-info"
popd
popd

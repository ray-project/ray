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
MODIN_VERSION=0.5.0
MODIN_WHEELS_FNAME="modin-$MODIN_VERSION-py$PYTHON_VERSION-none-any.whl"
MODIN_WHEELS_URL="https://github.com/modin-project/modin/releases/download/v$MODIN_VERSION/"

pushd $TP_DIR/../python/ray/
rm -rf modin
mkdir modin
pushd modin
curl -kL --silent "$MODIN_WHEELS_URL$MODIN_WHEELS_FNAME" -o "$MODIN_WHEELS_FNAME"
unzip -qq "$MODIN_WHEELS_FNAME"
rm "$MODIN_WHEELS_FNAME"
popd
popd

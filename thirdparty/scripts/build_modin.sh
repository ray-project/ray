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
if [[ $PYTHON_VERSION -eq "2" ]]; then
    MODIN_PYPI="https://files.pythonhosted.org/packages/f9/ee/5b0e73dddcbaed1f7585838c2d5092b0e7997f6fd0eb08b8d475a5326cb7/"
else
    MODIN_PYPI="https://files.pythonhosted.org/packages/1e/73/c5e428e7d58cbc6243c1acb1329103c8138e8471b6c92e4451434e7bb753/"
fi


pushd $TP_DIR/../python/ray/
rm -rf modin
mkdir modin
pushd modin
wget --no-check-certificate "$MODIN_PYPI$MODIN_WHEELS"
unzip $MODIN_WHEELS
rm $MODIN_WHEELS
rm -r "modin-$MODIN_VERSION.dist-info"
popd
popd

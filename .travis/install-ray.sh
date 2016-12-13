#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

if [[ "$PYTHON" == "2.7" ]]; then
  pushd "../numbuf"
    sudo python setup.py install
  popd

  pushd "../src/common/lib/python"
    sudo python setup.py install
  popd

  pushd "../lib/python"
    sudo python setup.py install
  popd
elif [[ "$PYTHON" == "3.5" ]]; then
  pushd "../numbuf"
    python setup.py install
  popd

  pushd "../src/common/lib/python"
    python setup.py install
  popd

  pushd "../lib/python"
    python setup.py install
  popd
else
  echo "Unrecognized Python version."
  exit 1
fi

pushd "../src/common"
  make test
popd

pushd "../src/plasma"
  make test
popd

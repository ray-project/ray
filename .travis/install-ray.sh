#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

if [[ "$PYTHON" == "2.7" ]]; then

  pushd "$ROOT_DIR/../src/common/lib/python"
    sudo python setup.py install
  popd

  pushd "$ROOT_DIR/../lib/python"
    sudo python setup.py install
  popd

elif [[ "$PYTHON" == "3.5" ]]; then
  export PATH="$HOME/miniconda/bin:$PATH"

  pushd "$ROOT_DIR/../src/common/lib/python"
    python setup.py install --user
  popd

  pushd "$ROOT_DIR/../lib/python"
    python setup.py install --user
  popd

else
  echo "Unrecognized Python version."
  exit 1
fi

#!/usr/bin/env bash

# Cause the script to exit if a single command fails
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

cython_examples="$ROOT_DIR/../../doc/examples/cython"

if [[ "$PYTHON" == "3.6" ]]; then
   export PATH="$HOME/miniconda/bin:$PATH"

   pushd $cython_examples
   pip install scipy
   python setup.py install --user
   popd

elif [[ "$LINT" == "1" ]]; then
   export PATH="$HOME/miniconda/bin:$PATH"

   pushd $cython_examples
   python setup.py install --user
   popd

else
   echo "Unrecognized Python version."
   exit 1
fi

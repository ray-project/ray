#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

# If we are in Travis, most of the compilation result will be cached.
# This means we are I/O bounded. By default, Bazel set the number of concurrent
# jobs to the the number cores on the machine, which are not efficient for
# network bounded cache downloading workload. Therefore we increase the number
# of jobs to 50
if [[ "$TRAVIS" == "true" ]]; then
  echo "build --jobs=50" >> $HOME/.bazelrc
fi

if [[ "$PYTHON" == "3.6" ]]; then
  export PATH="$HOME/miniconda/bin:$PATH"

  pushd "$ROOT_DIR/../../python"
    pushd ray/dashboard/client
      source $HOME/.nvm/nvm.sh
      nvm use node
      npm ci
      npm run build
    popd
    pip install -e . --verbose
  popd

elif [[ "$LINT" == "1" ]]; then
  export PATH="$HOME/miniconda/bin:$PATH"

  pushd "$ROOT_DIR/../../python"
    pip install -e . --verbose
  popd
else
  echo "Unrecognized Python version."
  exit 1
fi


#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON env var is set to $PYTHON, actual version is: "
# Print out actual version.
python -V

# If we are in Travis, most of the compilation result will be cached.
# This means we are I/O bounded. By default, Bazel set the number of concurrent
# jobs to the the number cores on the machine, which are not efficient for 
# network bounded cache downloading workload. Therefore we increase the number
# of jobs to 50
if [[ "$TRAVIS" == "true" ]]; then
  echo "build --jobs=50" >> $HOME/.bazelrc
fi

export PATH="$HOME/miniconda/bin:$PATH"

echo "npm version = "
npm version
npm install -g npm@latest

pushd "$ROOT_DIR/../../python"
  pushd ray/dashboard/client
    source $HOME/.nvm/nvm.sh
    nvm use node
    npm ci
    npm run build
  popd
  python setup.py install --user
popd

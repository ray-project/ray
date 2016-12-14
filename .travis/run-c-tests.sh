#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

if [[ "$PYTHON" == "3.5" ]]; then export PATH="$HOME/miniconda/bin:$PATH"; fi

pushd "$ROOT_DIR/../src/common"
  make test
popd

pushd "$ROOT_DIR/../src/plasma"
  make test
popd

pushd "$ROOT_DIR/../src/photon"
  make test
popd

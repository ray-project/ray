#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

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

else
  echo "Unrecognized Python version."
  exit 1
fi


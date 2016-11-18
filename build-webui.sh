#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  platform="linux"
elif [[ "$unamestr" == "Darwin" ]]; then
  platform="macosx"
else
  echo "Unrecognized platform."
  exit 1
fi

WEBUI_DIR="$ROOT_DIR/webui"
PYTHON_DIR="$ROOT_DIR/lib/python"

pushd "$WEBUI_DIR"
  npm install
  if [[ $platform == "linux" ]]; then
    nodejs ./node_modules/.bin/webpack -g
  elif [[ $platform == "macosx" ]]; then
    node ./node_modules/.bin/webpack -g
  fi
  pushd node_modules
    rm -rf react* babel* classnames dom-helpers jsesc webpack .bin
  popd
popd

cp -r $WEBUI_DIR $PYTHON_DIR

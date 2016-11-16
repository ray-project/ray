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

pushd "$WEBUI_DIR"
  npm install
  if [[ $platform == "linux" ]]; then
    nodejs ./node_modules/.bin/webpack -g
  elif [[ $platform == "macosx" ]]; then
    node ./node_modules/.bin/webpack -g
  fi
  pushd node_modules
    sudo rm -rf react* babel* classnames dom-helpers jsesc webpack .bin
  popd
popd

mkdir "$ROOT_DIR/lib/python/webui"
cp -r $WEBUI_DIR "$ROOT_DIR/lib/python"

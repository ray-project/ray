#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

mkdir -p "$ROOT_DIR/build"
pushd "$ROOT_DIR/build"
  cmake ..
  make install
popd

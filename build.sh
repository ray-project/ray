#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# Determine how many parallel jobs to use for make based on the number of cores
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  PARALLEL=$(nproc)
elif [[ "$unamestr" == "Darwin" ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo "Unrecognized platform."
  exit 1
fi

COMMON_DIR="$ROOT_DIR/src/common"
PLASMA_DIR="$ROOT_DIR/src/plasma"
PHOTON_DIR="$ROOT_DIR/src/photon"

PYTHON_DIR="$ROOT_DIR/lib/python"
PYTHON_COMMON_DIR="$PYTHON_DIR/common"
PYTHON_PLASMA_DIR="$PYTHON_DIR/plasma"
PYTHON_PHOTON_DIR="$PYTHON_DIR/photon"

pushd "$COMMON_DIR"
  make
  make test
popd
cp "$COMMON_DIR/thirdparty/redis-3.2.3/src/redis-server" "$PYTHON_COMMON_DIR/thirdparty/redis-3.2.3/src/"

pushd "$PLASMA_DIR"
  make
  make test
  pushd "$PLASMA_DIR/build"
    cmake ..
    make install
  popd
popd
cp "$PLASMA_DIR/build/plasma_store" "$PYTHON_PLASMA_DIR/build/"
cp "$PLASMA_DIR/build/plasma_manager" "$PYTHON_PLASMA_DIR/build/"
cp "$PLASMA_DIR/build/plasma_client.so" "$PYTHON_PLASMA_DIR/build/"
cp "$PLASMA_DIR/lib/python/plasma.py" "$PYTHON_PLASMA_DIR/lib/python/"
cp "$PLASMA_DIR/build/libplasma.so" "$PYTHON_PLASMA_DIR/lib/python"

pushd "$PHOTON_DIR"
  make
  pushd "$PHOTON_DIR/build"
    cmake ..
    make install
  popd
popd
cp "$PHOTON_DIR/build/photon_scheduler" "$PYTHON_PHOTON_DIR/build"
cp "$PHOTON_DIR/photon/libphoton.so" "$PYTHON_PHOTON_DIR/"

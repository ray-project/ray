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

# Build plasma. We currently have to do this because we compile numbuf with the
# HAS_PLASMA flag.
PLASMA_DIR="$ROOT_DIR/../src/plasma"
pushd "$PLASMA_DIR"
  make clean
  make
popd

mkdir -p "$ROOT_DIR/build"
pushd "$ROOT_DIR/build"
  cmake -DHAS_PLASMA=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="-g" -DCMAKE_CXX_FLAGS="-g" ..
  make install -j$PARALLEL
popd

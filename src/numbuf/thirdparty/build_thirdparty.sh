#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
PREFIX=$TP_DIR/installed

# Determine how many parallel jobs to use for make based on the number of cores
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  PARALLEL=$(nproc)
elif [[ "$unamestr" == "Darwin" ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
  echo "Platform is macosx."
else
  echo "Unrecognized platform."
  exit 1
fi

echo "building arrow"
cd $TP_DIR/arrow/cpp
mkdir -p $TP_DIR/arrow/cpp/build
cd $TP_DIR/arrow/cpp/build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="-g -lpthread" -DCMAKE_CXX_FLAGS="-g -lpthread" -DARROW_BUILD_TESTS=OFF ..
make clean
make VERBOSE=1 -j$PARALLEL

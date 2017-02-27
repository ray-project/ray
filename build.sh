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

pushd "$ROOT_DIR/src/common/thirdparty/"
  bash build-redis.sh
popd

bash "$ROOT_DIR/src/numbuf/thirdparty/download_thirdparty.sh"
bash "$ROOT_DIR/src/numbuf/thirdparty/build_thirdparty.sh"

# Now build everything.
pushd "$ROOT_DIR/python/core"
  cmake -DCMAKE_BUILD_TYPE=Release  ../..
  make clean
  make
popd

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

bash "$ROOT_DIR/src/thirdparty/download_thirdparty.sh"
bash "$ROOT_DIR/src/thirdparty/build_thirdparty.sh"

# Now build everything.
pushd "$ROOT_DIR/python/ray/core"
  # We use these variables to set PKG_CONFIG_PATH, which is important so that
  # in cmake, pkg-config can find plasma.
  TP_DIR=$ROOT_DIR/src/thirdparty
  ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install
  if [ "$VALGRIND" = "1" ]
  then
    PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig cmake -DCMAKE_BUILD_TYPE=Debug ../../..
  else
    PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig cmake -DCMAKE_BUILD_TYPE=Release ../../..
  fi
  make clean
  make -j${PARALLEL}
popd

# Move stuff from Arrow to Ray.

mv $ROOT_DIR/src/thirdparty/arrow/cpp/build/release/plasma_store $ROOT_DIR/python/ray/core/src/plasma/

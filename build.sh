#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

bash $ROOT_DIR/setup_thirdparty.sh $PYTHON_EXECUTABLE

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

# Now we build everything.
pushd "$ROOT_DIR/python/ray/core"
  # We use these variables to set PKG_CONFIG_PATH, which is important so that
  # in cmake, pkg-config can find plasma.
  TP_PKG_DIR=$ROOT_DIR/thirdparty/pkg
  ARROW_HOME=$TP_PKG_DIR/arrow/cpp/build/cpp-install
  if [[ "$VALGRIND" = "1" ]]; then
    BOOST_ROOT=$TP_PKG_DIR/boost \
    PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
    cmake -DCMAKE_BUILD_TYPE=Debug \
          -DRAY_USE_NEW_GCS=$RAY_USE_NEW_GCS \
          -DPYTHON_EXECUTABLE:FILEPATH=$PYTHON_EXECUTABLE \
          ../../..
  else
    BOOST_ROOT=$TP_PKG_DIR/boost \
    PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DRAY_USE_NEW_GCS=$RAY_USE_NEW_GCS \
          -DPYTHON_EXECUTABLE:FILEPATH=$PYTHON_EXECUTABLE \
          ../../..
  fi
  make clean
  make -j${PARALLEL}
popd

# Move stuff from Arrow to Ray.
cp $ROOT_DIR/thirdparty/pkg/arrow/cpp/build/cpp-install/bin/plasma_store $ROOT_DIR/python/ray/core/src/plasma/

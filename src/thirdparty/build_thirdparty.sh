#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

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
export ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install

# Get the directory of the Python executable.
PYTHON_EXECUTABLE_DIR=$(dirname $PYTHON_EXECUTABLE)

# Pass a slightly different path into this command so that cmake finds the right
# Python interpreter and libraries.
PATH=$PYTHON_EXECUTABLE_DIR:$PATH \
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_FLAGS="-g -O3" \
      -DCMAKE_CXX_FLAGS="-g -O3" \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_BUILD_TESTS=off \
      -DARROW_HDFS=on \
      -DARROW_PYTHON=on \
      -DARROW_PLASMA=on \
      -DPLASMA_PYTHON=on \
      -DARROW_JEMALLOC=off \
      -DARROW_WITH_BROTLI=off \
      -DARROW_WITH_LZ4=off \
      -DARROW_WITH_SNAPPY=off \
      -DARROW_WITH_ZLIB=off \
      -DARROW_WITH_ZSTD=off \
      ..
make VERBOSE=1 -j$PARALLEL
make install

echo "installing pyarrow"
cd $TP_DIR/arrow/python
# We set PKG_CONFIG_PATH, which is important so that in cmake, pkg-config can
# find plasma.
ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install
PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig PYARROW_WITH_PLASMA=1 PYARROW_BUNDLE_ARROW_CPP=1 $PYTHON_EXECUTABLE setup.py install

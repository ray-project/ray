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

# If we're on Linux, then compile boost. This installs boost to $TP_DIR/boost.
if [[ "$unamestr" == "Linux" ]]; then
  echo "building boost"
  bash "$TP_DIR/build_boost.sh"
fi

# If we're on Linux, then compile flatbuffers. This installs flatbuffers to
# $TP_DIR/flatbuffers.
if [[ "$unamestr" == "Linux" ]]; then
  echo "building flatbuffers"
  bash "$TP_DIR/build_flatbuffers.sh"
  FLATBUFFERS_HOME=$TP_DIR/flatbuffers
else
  FLATBUFFERS_HOME=""
fi

# Clone catapult and build the static HTML needed for the UI.
bash "$TP_DIR/build_ui.sh"

echo "building arrow"
cd $TP_DIR/arrow/cpp
mkdir -p $TP_DIR/arrow/cpp/build
cd $TP_DIR/arrow/cpp/build
ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install

BOOST_ROOT=$TP_DIR/boost \
FLATBUFFERS_HOME=$FLATBUFFERS_HOME \
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_FLAGS="-g -O3" \
      -DCMAKE_CXX_FLAGS="-g -O3" \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_BUILD_TESTS=off \
      -DARROW_HDFS=on \
      -DARROW_BOOST_USE_SHARED=off \
      -DPYTHON_EXECUTABLE:FILEPATH=$PYTHON_EXECUTABLE \
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

if [[ -d $ARROW_HOME/lib64 ]]; then
  # On CentOS, Arrow gets installed under lib64 instead of lib, so copy it for
  # now. TODO(rkn): A preferable solution would be to add both directories to
  # the PKG_CONFIG_PATH, but that didn't seem to work.
  cp -r $ARROW_HOME/lib64 $ARROW_HOME/lib
fi

echo "installing pyarrow"
cd $TP_DIR/arrow/python
# We set PKG_CONFIG_PATH, which is important so that in cmake, pkg-config can
# find plasma.
PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
PYARROW_WITH_PLASMA=1 \
PYARROW_BUNDLE_ARROW_CPP=1 \
$PYTHON_EXECUTABLE setup.py build
PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
PYARROW_WITH_PLASMA=1 \
PYARROW_BUNDLE_ARROW_CPP=1 \
$PYTHON_EXECUTABLE setup.py build_ext
# Find the pyarrow directory that was just built and copy it to ray/python/ray/
# so that pyarrow can be packaged along with ray.
pushd .
cd $TP_DIR/arrow/python/build
PYARROW_BUILD_LIB_DIR="$TP_DIR/arrow/python/build/$(find ./ -maxdepth 1 -type d -print | grep -m1 'lib')"
popd
echo "copying pyarrow files from $PYARROW_BUILD_LIB_DIR/pyarrow"
cp -r $PYARROW_BUILD_LIB_DIR/pyarrow $TP_DIR/../../python/ray/pyarrow_files/

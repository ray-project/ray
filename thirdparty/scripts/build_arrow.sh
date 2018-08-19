#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../

# setup env
if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

unamestr="$(uname)"

if [[ "$unamestr" == "Linux" ]]; then
  FLATBUFFERS_HOME=$TP_DIR/pkg/flatbuffers
else
  FLATBUFFERS_HOME=""
fi

# Determine how many parallel jobs to use for make based on the number of cores
if [[ "$unamestr" == "Linux" ]]; then
  PARALLEL=$(nproc)
elif [[ "$unamestr" == "Darwin" ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
  echo "Platform is macosx."
else
  echo "Unrecognized platform."
  exit 1
fi

# The PR for this commit is https://github.com/apache/arrow/pull/2368. We
# include the link here to make it easier to find the right commit because
# Arrow often rewrites git history and invalidates certain commits.
TARGET_COMMIT_ID=4660833b2c5ef63a97445e304b8f72a2e0170f9c
build_arrow() {
  echo "building arrow"
  # Make sure arrow will be built again when building ray for java later than python
  if [[ "$RAY_BUILD_JAVA" == "YES" ]]; then
    rm -rf $TP_DIR/build/arrow/cpp/build/CMakeCache.txt
  fi

  if [[ ! -d $TP_DIR/build/arrow ]]; then
    git clone -q https://github.com/apache/arrow.git "$TP_DIR/build/arrow"
  fi

  if ! [ -x "$(command -v bison)" ]; then
    echo 'Error: bison is not installed.' >&2
    exit 1
  fi

  if ! [ -x "$(command -v flex)" ]; then
    echo 'Error: flex is not installed.' >&2
    exit 1
  fi

  pushd $TP_DIR/build/arrow
  git fetch origin master

  git checkout $TARGET_COMMIT_ID

  cd cpp
  if [ ! -d "build" ]; then
    mkdir build
  fi
  cd build

  BUILD_ARROW_PLASMA_JAVA_CLIENT=off
  if [[ "$RAY_BUILD_JAVA" == "YES" ]]; then
    BUILD_ARROW_PLASMA_JAVA_CLIENT=on
  fi

  # Clean the build cache for arrow and parquet, in case the error of "Cannot find Parquet" occurs.
  rm -rf $TP_DIR/build/arrow/python/build/temp*

  ARROW_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install
  BOOST_ROOT=$TP_DIR/pkg/boost \
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
      -DARROW_TENSORFLOW=on \
      -DARROW_JEMALLOC=off \
      -DARROW_WITH_BROTLI=off \
      -DARROW_WITH_LZ4=off \
      -DARROW_WITH_ZLIB=off \
      -DARROW_WITH_ZSTD=off \
      -DARROW_PLASMA_JAVA_CLIENT=$BUILD_ARROW_PLASMA_JAVA_CLIENT \
      ..
  make VERBOSE=1 -j$PARALLEL
  make install

  if [[ -d $ARROW_HOME/lib64 ]]; then
      # On CentOS, Arrow gets installed under lib64 instead of lib, so copy it for
      # now. TODO(rkn): A preferable solution would be to add both directories to
      # the PKG_CONFIG_PATH, but that didn't seem to work.
      cp -r $ARROW_HOME/lib64 $ARROW_HOME/lib
  fi

  bash "$TP_DIR/scripts/build_parquet.sh"

  echo "installing pyarrow"
  cd $TP_DIR/build/arrow/python
  # We set PKG_CONFIG_PATH, which is important so that in cmake, pkg-config can
  # find plasma.
  PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
  PYARROW_WITH_PLASMA=1 \
  PYARROW_WITH_TENSORFLOW=1 \
  PYARROW_BUNDLE_ARROW_CPP=1 \
  $PYTHON_EXECUTABLE setup.py build

  PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
  PYARROW_WITH_PLASMA=1 \
  PYARROW_WITH_TENSORFLOW=1 \
  PYARROW_BUNDLE_ARROW_CPP=1 \
  PARQUET_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install \
  PYARROW_WITH_PARQUET=1 \
  $PYTHON_EXECUTABLE setup.py build_ext

  # Find the pyarrow directory that was just built and copy it to ray/python/ray/
  # so that pyarrow can be packaged along with ray.
  pushd $TP_DIR/build/arrow/python/build
  PYARROW_BUILD_LIB_DIR="$TP_DIR/build/arrow/python/build/$(find ./ -maxdepth 1 -type d -print | grep -m1 'lib')"
  popd
  echo "copying pyarrow files from $PYARROW_BUILD_LIB_DIR/pyarrow"
  cp -r $PYARROW_BUILD_LIB_DIR/pyarrow $TP_DIR/../python/ray/pyarrow_files/

  popd
}
# Download and compile arrow if it isn't already present or the commit-id mismatches.
if [[ "$RAY_BUILD_PYTHON" == "YES" && ! -d $TP_DIR/../python/ray/pyarrow_files/pyarrow ]] || \
    [[ "$RAY_BUILD_JAVA" == "YES" && ! -f $TP_DIR/build/arrow/cpp/build/release/libplasma_java.dylib ]]; then
  build_arrow
else
  REBUILD=off
  pushd $TP_DIR/build/arrow
  if [[ "$TARGET_COMMIT_ID" != `git rev-parse HEAD` ]]; then
    # TARGET_COMMIT_ID may change to later commit.
    echo "Commit ID mismatches."
    git fetch origin master
    git checkout $TARGET_COMMIT_ID
    REBUILD=on
  fi
  popd

  if [[ "$REBUILD" == "on" ]]; then
    build_arrow
  fi
fi

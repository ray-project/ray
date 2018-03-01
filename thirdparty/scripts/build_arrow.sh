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

# Download and compile arrow if it isn't already present.
if [[ ! -d $TP_DIR/pkg/arrow ]]; then
    echo "building arrow"
    git clone https://github.com/apache/arrow.git "$TP_DIR/build/arrow"

    pushd $TP_DIR/build/arrow
    git fetch origin master
    # The PR for this commit is https://github.com/apache/arrow/pull/1581. We
    # include the link here to make it easier to find the right commit because
    # Arrow often rewrites git history and invalidates certain commits.
    git checkout 46aa99e9843ac0148357bb36a9235cfd48903e73

    cd cpp
    mkdir build
    cd build

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
        -DPLASMA_PYTHON=on \
        -DARROW_JEMALLOC=off \
        -DARROW_WITH_BROTLI=off \
        -DARROW_WITH_LZ4=off \
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

    bash "$TP_DIR/scripts/build_parquet.sh"

    echo "installing pyarrow"
    cd $TP_DIR/build/arrow/python
    # We set PKG_CONFIG_PATH, which is important so that in cmake, pkg-config can
    # find plasma.
    PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
    PYARROW_WITH_PLASMA=1 \
    PYARROW_BUNDLE_ARROW_CPP=1 \
    $PYTHON_EXECUTABLE setup.py build

    PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig \
    PYARROW_WITH_PLASMA=1 \
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
fi

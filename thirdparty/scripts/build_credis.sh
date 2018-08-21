#!/usr/bin/env bash

# Usage: by default jemalloc is used; however, if tcmalloc is installed on the
# system, credis' CMakeLists.txt will prefer it over jemalloc.  To avoid build
# failures use:
#
#     CREDIS_TCMALLOC=1 build_credis.sh
#
# If building all of ray from ray/python, use:
#
#     env CREDIS_TCMALLOC=1 RAY_USE_NEW_GCS=on pip install -e . --verbose

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../
ROOT_DIR=$TP_DIR/..

# For some reason, on Ubuntu/gcc toolchain linking against static libleveldb.a
# doesn't work, so we force building the shared library for non-Mac.
if [ "$(uname)" == "Darwin" ]; then
    BUILD_LEVELDB_CONFIG=""
else
    BUILD_LEVELDB_CONFIG="-DBUILD_SHARED_LIBS=on"
fi

if [[ "${RAY_USE_NEW_GCS}" = "on" ]]; then
  pushd "$TP_DIR/pkg/"
    if [[ ! -d "credis" ]]; then
      git clone -q --recursive https://github.com/ray-project/credis
    fi
  popd

  pushd "$TP_DIR/pkg/credis"
    git fetch origin master
    git checkout 273d667e5126c246b45f5dcf030b651a653136c3

    # If the above commit points to different submodules' commits than
    # origin's head, this updates the submodules.
    git submodule update

    # TODO(pcm): Get the build environment for tcmalloc set up and compile redis
    # with tcmalloc.
    # NOTE(zongheng): if we don't specify MALLOC=jemalloc, then build behiavors
    # differ between Mac (libc) and Linux (jemalloc)... This breaks our CMake
    # rules.
    if [[ "${CREDIS_TCMALLOC}" = 1 ]]; then
        echo "CREDIS_MALLOC is set, using tcmalloc to build redis"
        pushd redis && env USE_TCMALLOC=yes make -j && popd
    else
        pushd redis && make -j MALLOC=jemalloc && popd
    fi
    pushd glog; cmake -DWITH_GFLAGS=off . && make -j; popd
    pushd leveldb;
      mkdir -p build && cd build
      cmake ${BUILD_LEVELDB_CONFIG} -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
    popd

    mkdir -p build
    pushd build
      cmake -DCMAKE_BUILD_TYPE=Release ..
      make -j
    popd

    mkdir -p $ROOT_DIR/build/src/credis/redis/src/
    cp redis/src/redis-server $ROOT_DIR/build/src/credis/redis/src/redis-server

    mkdir -p $ROOT_DIR/build/src/credis/build/src/
    cp build/src/libmaster.so $ROOT_DIR/build/src/credis/build/src/libmaster.so
    cp build/src/libmember.so $ROOT_DIR/build/src/credis/build/src/libmember.so
  popd
fi

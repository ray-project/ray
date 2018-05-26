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

if [[ "${RAY_USE_NEW_GCS}" = "on" ]]; then
  pushd "$TP_DIR/pkg/"
    rm -rf credis
    git clone --recursive https://github.com/ray-project/credis
  popd

  pushd "$TP_DIR/pkg/credis"
    # 4/10/2018 credis/integrate branch.  With updated redis hacks.
    git checkout cbe8ade35d2278b1d94684fa5d00010cb015ef82

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
    pushd glog && cmake -DWITH_GFLAGS=off . && make -j && popd
    # NOTE(zongheng): DO NOT USE -j parallel build for leveldb as it's incorrect!
    pushd leveldb && CXXFLAGS="$CXXFLAGS -fPIC" make && popd

    mkdir build
    pushd build
      cmake ..
      make -j
    popd

    mkdir -p $ROOT_DIR/python/ray/core/src/credis/redis/src/
    cp redis/src/redis-server $ROOT_DIR/python/ray/core/src/credis/redis/src/redis-server
    mkdir -p $ROOT_DIR/python/ray/core/src/credis/build/src/
    cp build/src/libmaster.so $ROOT_DIR/python/ray/core/src/credis/build/src/libmaster.so
    cp build/src/libmember.so $ROOT_DIR/python/ray/core/src/credis/build/src/libmember.so
  popd
fi

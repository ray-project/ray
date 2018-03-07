#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../
ROOT_DIR=$TP_DIR/..

if [[ "${RAY_USE_NEW_GCS}" = "on" ]]; then
  if [ ! -d $TP_DIR/pkg/credis ]; then
    pushd "$TP_DIR/pkg/"
      rm -rf credis
      git clone --recursive https://github.com/ray-project/credis
    popd

    pushd "$TP_DIR/pkg/credis"
      git checkout 6be4a739ab5e795c98402b27c2e254f86e3524ea

      # TODO(pcm): Get the build environment for tcmalloc set up and compile redis
      # with tcmalloc.
      # NOTE(zongheng): if we don't specify MALLOC=jemalloc, then build behiavors
      # differ between Mac (libc) and Linux (jemalloc)... This breaks our CMake
      # rules.
      pushd redis && make -j MALLOC=jemalloc && popd
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
fi

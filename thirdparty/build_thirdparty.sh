#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
PREFIX=$TP_DIR/installed

# Determine how many parallel jobs to use for make based on the number of cores
if [[ "$OSTYPE" =~ ^linux ]]; then
  PARALLEL=$(grep -c processor /proc/cpuinfo)
elif [[ "$OSTYPE" == "darwin"* ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo Unsupported platform $OSTYPE
  exit 1
fi

echo "building arrow"
cd $TP_DIR/arrow/cpp
source setup_build_env.sh
mkdir -p $TP_DIR/arrow/cpp/build
cd $TP_DIR/arrow/cpp/build
cmake -DLIBARROW_LINKAGE=STATIC -DCMAKE_BUILD_TYPE=Release ..
make VERBOSE=1 -j$PARALLEL

echo "building GRPC"
cd $TP_DIR/grpc
make static HAS_SYSTEM_PROTOBUF=false HAS_SYSTEM_ZLIB=false -j$PARALLEL

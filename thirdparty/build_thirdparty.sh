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

echo "installing arrow"
export FLATBUFFERS_HOME=$TP_DIR/arrow/cpp/thirdparty/installed/
$TP_DIR/arrow/cpp/thirdparty/download_thirdparty.sh
$TP_DIR/arrow/cpp/thirdparty/build_thirdparty.sh
mkdir -p $TP_DIR/arrow/cpp/build
cd $TP_DIR/arrow/cpp/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make VERBOSE=1 -j$PARALLEL
sudo make VERBOSE=1 install

echo "installing numbuf"
mkdir -p $TP_DIR/numbuf/cpp/build
cd $TP_DIR/numbuf/cpp/build
cmake -DCMAKE_BUILD_TYPE=Release  ..
make VERBOSE=1 -j$PARALLEL
sudo make VERBOSE=1 install
mkdir -p $TP_DIR/numbuf/python/build
cd $TP_DIR/numbuf/python/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make VERBOSE=1 -j$PARALLEL
sudo make VERBOSE=1 install

echo "installing GRPC"
cd $TP_DIR/grpc/third_party/protobuf
./autogen.sh
export CXXFLAGS="$CXXFLAGS -fPIC"
./configure --enable-static=no
sudo make install
sudo ldconfig
cd python
sudo python setup.py install
cd $TP_DIR/grpc
make VERBOSE=1 -j$PARALLEL
sudo make VERBOSE=1 install

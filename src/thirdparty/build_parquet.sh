#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

unamestr="$(uname)"
TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ "$unamestr" == "Darwin" ]; then
  brew update > /dev/null
  brew install boost && true
  brew install openssl
  brew install bison
  export OPENSSL_ROOT_DIR=/usr/local/opt/openssl
  export LD_LIBRARY_PATH=/usr/local/opt/openssl/lib:$LD_LIBRARY_PATH
  export PATH="/usr/local/opt/bison/bin:$PATH"
else
  export BOOST_ROOT=$TP_DIR/boost
fi

cd $TP_DIR/parquet-cpp
ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_BUILD_TESTS=off \
      .

make -j4
make install

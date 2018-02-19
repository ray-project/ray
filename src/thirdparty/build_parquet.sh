#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

unamestr="$(uname)"
TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

cd $TP_DIR/parquet-cpp

PARQUET_HOME=$TP_DIR/arrow/cpp/build/cpp-install

if [ "$unamestr" == "Darwin" ]; then
  ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install \
  OPENSSL_ROOT_DIR=/usr/local/opt/openssl \
  LD_LIBRARY_PATH=/usr/local/opt/openssl/lib:$LD_LIBRARY_PATH \
  PATH="/usr/local/opt/bison/bin:$PATH" \
  cmake -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
        -DPARQUET_BUILD_BENCHMARKS=off \
        -DPARQUET_BUILD_EXECUTABLES=off \
        -DPARQUET_BUILD_TESTS=off \
        .
else
  ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install \
  BOOST_ROOT=$TP_DIR/boost \
  cmake -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
        -DPARQUET_BUILD_BENCHMARKS=off \
        -DPARQUET_BUILD_EXECUTABLES=off \
        -DPARQUET_BUILD_TESTS=off \
        .
fi


make -j4
make install

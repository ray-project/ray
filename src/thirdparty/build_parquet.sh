#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

unamestr="$(uname)"
TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

cd $TP_DIR/parquet-cpp

PARQUET_HOME=$TP_DIR/arrow/cpp/build/cpp-install

OPENSSL_DIR=/usr/local/opt/openssl
BISON_DIR=/usr/local/opt/bison/bin

if [ "$unamestr" == "Darwin" ]; then
  OPENSSL_ROOT_DIR=$OPENSSL_DIR \
  PATH="$BISON_DIR:$PATH" \
  ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install \
  cmake -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
        -DPARQUET_BUILD_BENCHMARKS=off \
        -DPARQUET_BUILD_EXECUTABLES=off \
        -DPARQUET_BUILD_TESTS=off \
        .

  OPENSSL_ROOT_DIR=$OPENSSL_DIR \
  PATH="$BISON_DIR:$PATH" \
  make -j4

  OPENSSL_ROOT_DIR=$OPENSSL_DIR \
  PATH="$BISON_DIR:$PATH" \
  make install
else
  BOOST_ROOT=$TP_DIR/boost \
  ARROW_HOME=$TP_DIR/arrow/cpp/build/cpp-install \
  cmake -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
        -DPARQUET_BUILD_BENCHMARKS=off \
        -DPARQUET_BUILD_EXECUTABLES=off \
        -DPARQUET_BUILD_TESTS=off \
        .

  PARQUET_HOME=$TP_DIR/arrow/cpp/build/cpp-install \
  BOOST_ROOT=$TP_DIR/boost \
  make -j4

  make install
fi

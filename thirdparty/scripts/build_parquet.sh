#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

unamestr="$(uname)"
TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../
PARQUET_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install
OPENSSL_DIR=/usr/local/opt/openssl
BISON_DIR=/usr/local/opt/bison/bin

if [ ! -d $TP_DIR/build/parquet-cpp ]; then
  git clone https://github.com/apache/parquet-cpp.git "$TP_DIR/build/parquet-cpp"
  pushd $TP_DIR/build/parquet-cpp
  git fetch origin master
  git checkout 0875e43010af485e1c0b506d77d7e0edc80c66cc

  if [ "$unamestr" == "Darwin" ]; then
    OPENSSL_ROOT_DIR=$OPENSSL_DIR \
    PATH="$BISON_DIR:$PATH" \
    BOOST_ROOT=$TP_DIR/pkg/boost \
    ARROW_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
          -DPARQUET_BUILD_BENCHMARKS=off \
          -DPARQUET_BUILD_EXECUTABLES=off \
          -DPARQUET_BUILD_TESTS=off \
          .

    OPENSSL_ROOT_DIR=$OPENSSL_DIR \
    PATH="$BISON_DIR:$PATH" \
    #make -j4 # Parallel make might fail for low-spec machine.
    make

    OPENSSL_ROOT_DIR=$OPENSSL_DIR \
    PATH="$BISON_DIR:$PATH" \
    make install
  else
    BOOST_ROOT=$TP_DIR/pkg/boost \
    ARROW_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
          -DPARQUET_BUILD_BENCHMARKS=off \
          -DPARQUET_BUILD_EXECUTABLES=off \
          -DPARQUET_BUILD_TESTS=off \
          .

    PARQUET_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install \
    BOOST_ROOT=$TP_DIR/pkg/boost \
    #make -j4 # Parallel make might fail for low-spec machine.
    make
    make install
  fi

  popd
fi

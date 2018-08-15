#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

unamestr="$(uname)"
TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../
PARQUET_HOME=$TP_DIR/pkg/arrow/cpp/build/cpp-install
OPENSSL_DIR=/usr/local/opt/openssl
BISON_DIR=/usr/local/opt/bison/bin
TARGET_COMMIT_ID=63f41b00bddecb172bd5b3aa0366b4653f498811

build_parquet() {
  echo "Building Parquet"
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
    make -j4

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
    make -j4
    make install
  fi
}

if [ ! -d $TP_DIR/build/parquet-cpp ]; then
  git clone https://github.com/apache/parquet-cpp.git "$TP_DIR/build/parquet-cpp"
  pushd $TP_DIR/build/parquet-cpp
  git fetch origin master
  git checkout $TARGET_COMMIT_ID

  build_parquet
  popd
else
  pushd $TP_DIR/build/parquet-cpp
  if [[ "$TARGET_COMMIT_ID" != `git rev-parse HEAD` ]]; then
    # TARGET_COMMIT_ID may change to later commit.
    echo "Commit ID mismatches."
    git fetch origin master
    git checkout $TARGET_COMMIT_ID
    build_parquet
  fi

  popd
fi

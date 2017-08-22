#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# Download and compile boost if it isn't already present.
if [ ! -d $TP_DIR/boost ]; then
  wget --no-check-certificate http://downloads.sourceforge.net/project/boost/boost/1.60.0/boost_1_60_0.tar.gz -O $TP_DIR/boost_1_60_0.tar.gz
  tar xf $TP_DIR/boost_1_60_0.tar.gz -C $TP_DIR/
  rm -rf $TP_DIR/boost_1_60_0.tar.gz

  # Compile boost.
  pushd $TP_DIR/boost_1_60_0
    ./bootstrap.sh
    ./bjam cxxflags=-fPIC cflags=-fPIC --prefix=$TP_DIR/boost --with-filesystem --with-system install > /dev/null
  popd
fi

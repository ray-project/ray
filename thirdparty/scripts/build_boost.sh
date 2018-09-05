#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../

BOOST_VERSION=1.65.1
BOOST_VERSION_UNDERSCORE=1_65_1

# Download and compile boost if it isn't already present.
if [[ ! -d $TP_DIR/pkg/boost ]]; then
  # The wget command frequently fails, so retry up to 20 times.
  for COUNT in {1..20}; do
    # Attempt to wget boost and break from the retry loop if it succeeds.
    wget --progress=dot:giga --no-check-certificate http://dl.bintray.com/boostorg/release/$BOOST_VERSION/source/boost_$BOOST_VERSION_UNDERSCORE.tar.gz -O $TP_DIR/build/boost_$BOOST_VERSION_UNDERSCORE.tar.gz && break
    # If none of the retries succeeded at getting boost, then fail.
    if [[ $COUNT == 20 ]]; then
      exit 1
    fi
  done

  tar xf $TP_DIR/build/boost_$BOOST_VERSION_UNDERSCORE.tar.gz -C $TP_DIR/build/
  rm -rf $TP_DIR/build/boost_$BOOST_VERSION_UNDERSCORE.tar.gz

  # Compile boost.
  pushd $TP_DIR/build/boost_$BOOST_VERSION_UNDERSCORE
    ./bootstrap.sh
    ./bjam cxxflags=-fPIC cflags=-fPIC variant=release link=static --prefix=$TP_DIR/pkg/boost --with-filesystem --with-system --with-regex install > /dev/null
  popd
fi

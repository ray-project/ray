#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../

GTEST_VERSION=1.5.0
GTEST_VERSION_UNDERSCORE=1_5_0

# Download and compile gtest if it isn't already present.
if [[ ! -d $TP_DIR/pkg/gtest ]]; then
  # The wget command frequently fails, so retry up to 20 times.
  for COUNT in {1..20}; do
    # Attempt to wget gtest and break from the retry loop if it succeeds.
    wget --no-check-certificate https://github.com/google/googletest/archive/release-$GTEST_VERSION.tar.gz -O $TP_DIR/build/gtest_$GTEST_VERSION_UNDERSCORE.tar.gz && break
    # If none of the retries succeeded at getting gtest, then fail.
    if [[ $COUNT == 20 ]]; then
      exit 1
    fi
  done

  tar xf $TP_DIR/build/gtest_$GTEST_VERSION_UNDERSCORE.tar.gz -C $TP_DIR/build/
  rm -rf $TP_DIR/build/gtest_$GTEST_VERSION_UNDERSCORE.tar.gz

  # Compile gtest.
  mkdir -p $TP_DIR/pkg/gtest/lib
  pushd $TP_DIR/pkg/gtest/lib
    cp -r $TP_DIR/build/googletest-release-$GTEST_VERSION/include ../
    cmake $TP_DIR/build/googletest-release-$GTEST_VERSION
    make
  popd
fi

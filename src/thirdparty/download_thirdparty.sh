#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/apache/arrow.git "$TP_DIR/arrow"
fi
pushd $TP_DIR/arrow
  git fetch origin master
  # The PR for this commit is https://github.com/apache/arrow/pull/1581. We
  # include the link here to make it easier to find the right commit because
  # Arrow often rewrites git history and invalidates certain commits.
  git checkout 46aa99e9843ac0148357bb36a9235cfd48903e73
popd

if [ ! -d $TP_DIR/parquet-cpp ]; then
  git clone https://github.com/apache/parquet-cpp.git "$TP_DIR/parquet-cpp"
fi
pushd $TP_DIR/parquet-cpp
  git fetch origin master
  git checkout 76388ea4eb8b23656283116bc656b0c8f5db093b
popd

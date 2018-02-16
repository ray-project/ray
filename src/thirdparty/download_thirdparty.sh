#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/apache/arrow.git "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git fetch origin master
git checkout e26f3dad3675288564ef0c0330a5c9afcac652f1

cd $TP_DIR
if [ ! -d $TP_DIR/parquet-cpp ]; then
  git clone https://github.com/apache/parquet-cpp.git "$TP_DIR/parquet-cpp"
  pushd $TP_DIR/parquet-cpp
    git checkout 76388ea4eb8b23656283116bc656b0c8f5db093b
  popd
fi

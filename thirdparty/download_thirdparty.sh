#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d arrow ]; then
  echo "Fetching arrow"
  git clone https://github.com/apache/arrow.git
  cd arrow
  git checkout 4bd13b852d376065fdb16c36fa821ab0e167f0fc
  cd ..
fi

if [ ! -d numbuf ]; then
  echo "Fetching numbuf"
  git clone https://github.com/amplab/numbuf.git
fi

if [ ! -d grpc ]; then
  echo "Fetching GRPC"
  git clone https://github.com/grpc/grpc.git
  cd grpc
  git submodule update --init
  cd ..
fi

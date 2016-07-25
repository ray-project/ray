#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d arrow ]; then
  echo "Fetching arrow"
  git clone https://github.com/pcmoritz/arrow.git
  cd arrow
  git checkout scratch
  cd ..
fi

# TODO(pcm): Remove this
if [ ! -d arrow-old ]; then
  echo "Fetching old version of arrow"
  git clone https://github.com/pcmoritz/arrow.git arrow-old
  cd arrow-old
  git checkout static
  cd ..
fi

git submodule update --init --recursive

# this seems to be neeccessary for building on Mac OS X
cd grpc
git submodule update --init --recursive
cd ..

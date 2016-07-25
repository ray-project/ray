#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

git submodule update --init --recursive

# this seems to be neeccessary for building on Mac OS X
cd grpc
git submodule update --init --recursive
cd ..

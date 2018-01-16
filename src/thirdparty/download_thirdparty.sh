#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/robertnishihara/arrow.git "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git fetch origin master

git checkout 7f5c61873795bb9518573b96924228e337bf4f2b

#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/pcmoritz/arrow.git "$TP_DIR/arrow"
fi
cd "$TP_DIR/arrow"
git checkout a4a5526e4a8fbc4e4d5382a5c806ec871d2fbd9f

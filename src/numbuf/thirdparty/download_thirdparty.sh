#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/apache/arrow/ "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git checkout 15b874e47e3975c5240290ec7ed105bf8d1b56bc

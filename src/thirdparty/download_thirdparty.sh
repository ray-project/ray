#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/pcmoritz/arrow/ "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git fetch origin master

git checkout 5f7b779a5f07212f47f95330d5817fcfe2b13674

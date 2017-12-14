#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/pcmoritz/arrow.git "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git fetch origin master

git checkout 6a2a83ddff5605b598c8aa0ce99a5acaf500eae7

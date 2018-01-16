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

git checkout ab30d7d4718ab1d72f6f17e792c8bf20f49217d6

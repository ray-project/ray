#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/pcmoritz/arrow/ "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git pull origin master

git checkout a5b0b672842714cb9583bc51bff0ba5d1a95291c

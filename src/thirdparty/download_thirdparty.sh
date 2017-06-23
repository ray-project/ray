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

git checkout bf8552a6fc5662ebaf622b270b949be0abb9ba39

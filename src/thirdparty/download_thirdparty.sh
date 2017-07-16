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

git checkout 0c65b4cc1fc286b06ef5d848e02f0c3387ca722c

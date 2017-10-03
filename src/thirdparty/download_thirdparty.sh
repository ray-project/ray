#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/apache/arrow/ "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git fetch origin master

git checkout 988338c544580ffd367a5540f1061dd7b0fccc0e

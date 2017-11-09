#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/apache/arrow.git "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git fetch origin master

git remote add ray-arrow git@github.com:ray-project/arrow.git || true
git fetch ray-arrow
git checkout ray-arrow/plasma-client-disconnect-bug

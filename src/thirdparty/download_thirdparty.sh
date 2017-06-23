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

git checkout a23a0bb19bea9db05a7ca7b89c2bfee9375ae304

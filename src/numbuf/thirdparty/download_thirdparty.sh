#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/atumanov/arrow/ "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
#git checkout 98a52b4823f3cd0880eaef066dc932f533170292
git checkout parallel-arrow-rebase

#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ ! -d $TP_DIR/arrow ]; then
  git clone https://github.com/apache/arrow/ "$TP_DIR/arrow"
fi
cd $TP_DIR/arrow
git checkout af0c21e9a793c4cc9ec011d09cb18648678b5b43

#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

git clone https://github.com/pcmoritz/arrow.git "$TP_DIR/arrow"
cd "$TP_DIR/arrow"
git checkout 58bd7bedc63d66d5898297bab25b54dfb67665db

#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

git clone https://github.com/pcmoritz/arrow.git "$TP_DIR/arrow"
cd "$TP_DIR/arrow"
git checkout c88bd70c13cf16c07b840623cb466aa98d535be0

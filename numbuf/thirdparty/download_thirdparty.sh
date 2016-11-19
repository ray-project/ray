#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

git submodule update --init --recursive -- "$TP_DIR/arrow"

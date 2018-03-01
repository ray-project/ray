#!/bin/bash
set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
sh $TP_DIR/thirdparty/scripts/setup.sh

#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../
MODIN_VERSION=0.2.2
MODIN_REPO="https://github.com/modin-project/modin"

pushd $TP_DIR/../python/ray/
rm -rf modin
wget "$MODIN_REPO"/archive/v"$MODIN_VERSION".tar.gz
tar -xvzf v"$MODIN_VERSION".tar.gz
mv modin-"$MODIN_VERSION" modin
rm v"$MODIN_VERSION".tar.gz
popd

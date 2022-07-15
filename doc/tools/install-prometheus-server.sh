#!/usr/bin/env bash

set -x
set -e

TOOLS_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$TOOLS_DIR"

# Download Prometheus server.
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
    echo "Downloading Prometheus server for linux system."
    PACKAGE_NAME=prometheus-2.8.0.linux-amd64
elif [[ "$unamestr" == "Darwin" ]]; then
    echo "Downloading Prometheus server for MacOS system."
    PACKAGE_NAME=prometheus-2.8.0.darwin-amd64
else
    echo "Downloading abort: Unrecognized platform."
    exit 1
fi

URL=https://github.com/prometheus/prometheus/releases/download/v2.8.0/$PACKAGE_NAME.tar.gz
wget $URL
tar xvfz $PACKAGE_NAME.tar.gz

popd

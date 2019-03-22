#!/usr/bin/env bash

set -x
set -e

TOOLS_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd $TOOLS_DIR

# Download Prometheus server.
if [[ "$OSTYPE" == "linux"* ]]; then
    echo "Downloading Prometheus server for linux system."
    wget https://github.com/prometheus/prometheus/releases/download/v2.8.0/prometheus-2.8.0.linux-amd64.tar.gz
elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Downloading Prometheus server for MacOS system."
    wget https://github.com/prometheus/prometheus/releases/download/v2.8.0/prometheus-2.8.0.darwin-amd64.tar.gz
else
    echo "Downloading abort: un-support system."
fi

tar xvfz prometheus-*.tar.gz

popd

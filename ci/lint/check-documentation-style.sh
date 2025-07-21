#!/usr/bin/env bash

set -euxo pipefail

VALE_BIN=$(mktemp -d)
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    wget https://github.com/errata-ai/vale/releases/download/v3.4.1/vale_3.4.1_Linux_64-bit.tar.gz -P "$VALE_BIN"
    tar -xvzf "$VALE_BIN"/vale_3.4.1_Linux_64-bit.tar.gz -C "$VALE_BIN" vale
elif [[ "$OSTYPE" == "darwin"* ]]; then
    wget https://github.com/errata-ai/vale/releases/download/v3.4.1/vale_3.4.1_macOS_arm64.tar.gz -P "$VALE_BIN"
    tar -xvzf "$VALE_BIN"/vale_3.4.1_macOS_arm64.tar.gz -C "$VALE_BIN" vale
else
    echo "Unsupported OS: $OSTYPE"
    exit 1
fi
"$VALE_BIN"/vale doc/source/data doc/source/ray-overview/examples
rm -rf "$VALE_BIN"

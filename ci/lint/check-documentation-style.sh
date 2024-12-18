#!/usr/bin/env bash

set -euxo pipefail

VALE_BIN=$(mktemp -d)
wget https://github.com/errata-ai/vale/releases/download/v3.4.1/vale_3.4.1_Linux_64-bit.tar.gz -P "$VALE_BIN"
tar -xvzf "$VALE_BIN"/vale_3.4.1_Linux_64-bit.tar.gz -C "$VALE_BIN" vale
"$VALE_BIN"/vale doc/source/data
rm -rf "$VALE_BIN"

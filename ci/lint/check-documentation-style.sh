#!/usr/bin/env bash

set -euxo pipefail

wget https://github.com/errata-ai/vale/releases/download/v2.28.0/vale_2.28.0_Linux_64-bit.tar.gz
tar -xvzf vale_2.28.0_Linux_64-bit.tar.gz -C .
vale doc/source
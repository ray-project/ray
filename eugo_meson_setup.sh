#!/usr/bin/env bash
set -euo pipefail


export CFLAGS="-Wno-deprecated-declarations -Wno-deprecated-this-capture -Wno-unused-const-variable"
export CXXFLAGS="-Wno-deprecated-declarations -Wno-deprecated-this-capture -Wno-unused-const-variable"

export NODE_EXTRA_CA_CERTS="$(python3 -m certifi)"


meson setup --reconfigure eugo_build

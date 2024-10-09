#!/usr/bin/env bash
set -euo pipefail

export CFLAGS="-Wno-deprecated-declarations -Wno-deprecated-this-capture"
export CXXFLAGS="-Wno-deprecated-declarations -Wno-deprecated-this-capture"

meson setup --reconfigure eugo_build

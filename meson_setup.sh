#!/usr/bin/env bash
set -euo pipefail

export CFLAGS="-Wno-deprecated-declarations"
export CXXFLAGS="-Wno-deprecated-declarations"

meson setup --reconfigure eugo_build

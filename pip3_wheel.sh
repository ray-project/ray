#!/usr/bin/env bash
set -euo pipefail

export CFLAGS="-Wno-deprecated-declarations -Wno-deprecated-this-capture"
export CXXFLAGS="-Wno-deprecated-declarations -Wno-deprecated-this-capture"

pip3 wheel . --no-cache-dir --no-build-isolation --no-deps
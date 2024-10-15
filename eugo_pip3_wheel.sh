#!/usr/bin/env bash
set -euo pipefail


export CFLAGS="-Wno-deprecated-declarations -Wno-deprecated-this-capture"
export CXXFLAGS="-Wno-deprecated-declarations -Wno-deprecated-this-capture"

export PIP_NO_CLEAN=1

export NODE_EXTRA_CA_CERTS="$(python3 -m certifi)"


pip3 wheel . \
  -Cbuilddir=eugo_build_whl \
  --no-cache-dir \
  --no-build-isolation \
  --no-deps
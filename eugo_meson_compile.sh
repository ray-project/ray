#!/usr/bin/env bash
set -euo pipefail


export NODE_EXTRA_CA_CERTS="$(python3 -m certifi)"


meson compile -C eugo_build

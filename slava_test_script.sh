#!/usr/bin/env bash
set -euo pipefail

#export opencensus_DIR="/usr/local"
#export opencensus_INCLUDE_DIR="/usr/local/include"

# cmake --find-package -DNAME=opencensus -DCOMPILER_ID=clang -DLANGUAGE=CXX -DMODE=EXIST --debug-find
# cmake --find-package -DNAME=OpenCensus -DCOMPILER_ID=clang -DLANGUAGE=CXX -DMODE=EXIST --debug-find

meson setup --reconfigure eugo_build
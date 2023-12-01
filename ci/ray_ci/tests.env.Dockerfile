# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG BUILD_TYPE

ENV CC=clang
ENV CXX=clang++-12

RUN mkdir /rayci
WORKDIR /rayci
COPY . .

RUN <<EOF
#!/bin/bash -i

set -euo pipefail

if [[ "$BUILD_TYPE" == "doc" ]]; then
  # Do nothing in this case; no need to install ray to build docs
  :
else
  (
    cd dashboard/client
    npm ci
    npm run build
  )
  if [[ "$BUILD_TYPE" == "debug" ]]; then
    RAY_DEBUG_BUILD=debug pip install -v -e python/
  elif [[ "$BUILD_TYPE" == "asan" ]]; then
    pip install -v -e python/
    bazel build $(./ci/run/bazel_export_options) --no//:jemalloc_flag //:ray_pkg
  elif [[ "$BUILD_TYPE" == "java" ]]; then
    ./java/build-jar-multiplatform.sh linux
    RAY_INSTALL_JAVA=1 pip install -v -e python/
  elif [[ "$BUILD_TYPE" == "clang" || "$BUILD_TYPE" == "asan-clang" || "$BUILD_TYPE" == "tsan-clang" ]]; then
    ./ci/env/install-llvm-binaries.sh
    pip install -v -e python/
  else
    pip install -v -e python/
  fi
fi

EOF

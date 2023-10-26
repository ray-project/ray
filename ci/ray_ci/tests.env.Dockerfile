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
else
  pip install -v -e python/
fi

EOF

# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG BUILD_TYPE
ARG BUILDKITE_CACHE_READONLY
ARG RAY_INSTALL_MASK=

ENV CC=clang
ENV CXX=clang++-12

RUN mkdir /rayci
WORKDIR /rayci
COPY . .

RUN <<EOF
#!/bin/bash -i

set -euo pipefail

if [[ "$BUILDKITE_CACHE_READONLY" == "true" ]]; then
  # Disables uploading cache when it is read-only.
  echo "build --remote_upload_local_results=false" >> ~/.bazelrc
fi

if [[ "$BUILD_TYPE" == "skip" || "${BUILD_TYPE}" == "ubsan" ]]; then
  echo "Skipping building ray package"
  exit 0
fi

if [[ "$BUILD_TYPE" == "clang" || "$BUILD_TYPE" == "asan-clang" || "$BUILD_TYPE" == "tsan-clang" ]]; then
  echo "--- Install LLVM dependencies (and skip building ray package)"
  bash ci/env/install-llvm-binaries.sh
  exit 0
fi

if [[ "$RAY_INSTALL_MASK" != "" ]]; then
  echo "--- Apply mask: $RAY_INSTALL_MASK"
  if [[ "$RAY_INSTALL_MASK" =~ llm ]]; then
    rm -rf python/ray/llm
  fi
  if [[ "$RAY_INSTALL_MASK" =~ rllib ]]; then
    # Remove the actual directory and the symlink.
    rm -rf rllib python/ray/rllib
  fi
  if [[ "$RAY_INSTALL_MASK" =~ serve ]]; then
    rm -rf python/ray/serve
  fi
  if [[ "$RAY_INSTALL_MASK" =~ train ]]; then
    rm -rf python/ray/train
  fi
  if [[ "$RAY_INSTALL_MASK" =~ tune ]]; then
    rm -rf python/ray/tune
  fi
  if [[ "$RAY_INSTALL_MASK" =~ workflow ]]; then
    rm -rf python/ray/workflow
  fi
fi

echo "--- Build dashboard"

(
  cd python/ray/dashboard/client
  npm ci
  npm run build
)

echo "--- Install Ray with -e"

if [[ "$BUILD_TYPE" == "debug" ]]; then
  RAY_DEBUG_BUILD=debug pip install -v -e python/
elif [[ "$BUILD_TYPE" == "asan" ]]; then
  pip install -v -e python/
  bazel build $(./ci/run/bazel_export_options) --no//:jemalloc_flag //:ray_pkg
elif [[ "$BUILD_TYPE" == "java" ]]; then
  bash java/build-jar-multiplatform.sh linux
  RAY_INSTALL_JAVA=1 pip install -v -e python/
else
  pip install -v -e python/
fi

EOF

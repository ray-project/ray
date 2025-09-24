# syntax=docker/dockerfile:1.3-labs

ARG RAY_CORE_IMAGE=scratch
FROM "$RAY_CORE_IMAGE" AS ray_core

ARG RAY_DASHBOARD_IMAGE=scratch
FROM "$RAY_DASHBOARD_IMAGE" AS ray_dashboard

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG BUILD_TYPE
ARG BUILDKITE_CACHE_READONLY
ARG RAY_DISABLE_EXTRA_CPP=1
ARG RAY_INSTALL_MASK=

ENV CC=clang
ENV CXX=clang++-12
# Disabling C++ API build to speed up CI
# Only needed for java tests where we override this.
ENV RAY_DISABLE_EXTRA_CPP=${RAY_DISABLE_EXTRA_CPP}

RUN mkdir /rayci
WORKDIR /rayci
COPY . .

RUN --mount=type=bind,from=ray_core,target=/opt/ray-core \
    --mount=type=bind,from=ray_dashboard,target=/opt/ray-dashboard \
<<EOF
#!/bin/bash -i

set -euo pipefail

if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
  # Disables uploading cache when it is read-only.
  echo "build --remote_upload_local_results=false" >> ~/.bazelrc
fi

if [[ "$BUILD_TYPE" == "skip" || "${BUILD_TYPE}" == "ubsan" ]]; then
  echo "Skipping building ray package"
  exit 0
fi

if [[ "$BUILD_TYPE" == "clang" || "$BUILD_TYPE" == "asan-clang" || "$BUILD_TYPE" == "tsan-clang" || "$BUILD_TYPE" == "cgroup" ]]; then
  echo "--- Install LLVM dependencies (and skip building ray package)"
  bash ci/env/install-llvm-binaries.sh
  exit 0
fi

if [[ "$RAY_INSTALL_MASK" != "" ]]; then
  echo "--- Apply mask: $RAY_INSTALL_MASK"
  if [[ "$RAY_INSTALL_MASK" =~ all-ray-libraries ]]; then
    rm -rf python/ray/air
    rm -rf python/ray/data
    rm -rf python/ray/llm
    # Remove the actual directory and the symlink.
    rm -rf rllib python/ray/rllib
    rm -rf python/ray/serve
    rm -rf python/ray/train
    rm -rf python/ray/tune
    rm -rf python/ray/workflow
  fi
fi


if [[ -e /opt/ray-dashboard/dashboard.tar.gz ]]; then
  echo "--- Extract built dashboard"
  mkdir -p python/ray/dashboard/client/build
  tar -xzf /opt/ray-dashboard/dashboard.tar.gz -C python/ray/dashboard/client/build
else
  echo "--- Build dashboard"
  (
    cd python/ray/dashboard/client
    npm ci
    npm run build
  )
fi

echo "--- Install Ray with -e"

if [[ "$BUILD_TYPE" == "debug" ]]; then
  RAY_DEBUG_BUILD=debug pip install -v -e python/
elif [[ "$BUILD_TYPE" == "asan" ]]; then
  pip install -v -e python/
  bazel run $(./ci/run/bazel_export_options) --no//:jemalloc_flag //:gen_ray_pkg
elif [[ "$BUILD_TYPE" == "java" ]]; then
  bash java/build-jar-multiplatform.sh linux
  RAY_INSTALL_JAVA=1 pip install -v -e python/
else
  if [[ -e /opt/ray-core/ray_pkg.zip && "$BUILD_TYPE" == "optimized" && "$RAY_DISABLE_EXTRA_CPP" == "1" ]]; then
    echo "--- Extract built ray core bits"
    unzip -o -q /opt/ray-core/ray_pkg.zip -d python
    echo "--- Install Ray with -e"
    RAY_BUILD_CORE=0 pip install -v -e python/
  else
    # Fall back to normal path.
    echo "--- Install Ray with -e"
    pip install -v -e python/
  fi
fi

EOF

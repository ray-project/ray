# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG BUILD_TYPE
ARG BUILDKITE_CACHE_READONLY

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

if [[ "$BUILD_TYPE" == "skip" ]]; then
  echo "Skipping build"
  exit 0
fi

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

# TODO(aslonnie): fix this hack.
#
# It is required because in CI, we compile the wheel with
# the libstdc++ from the system. In the ideal world though, the wheel
# needs to be compiled in a manylinux compatible environment. Ubuntu 22.04 uses
# GLIBCXX_3.4.30 (libstdc++ 12.3.0), where the libstdc++ in conda environment
# only has up to GLIBCXX_3.4.29 . As a result, the compiled raylet does not run
# in the conda environment.
#
# For the right fix, we can use one of the following options:
#  - Compile ray package somewhere else, and copy/install it in.
#  - Downgrade the system libstdc++ to be compatible with the conda environment.
#  - Define the C++ toolchain with bazel so that it does not depend on the
#    system-wide toolchain anymroe.
#
# When released and used in production, we use the wheel built from manylinux,
# so it will not have this issue.
ln -sf /usr/lib/gcc/x86_64-linux-gnu/11/libstdc++.so /opt/miniconda/lib/libstdc++.so
ln -sf /usr/lib/gcc/x86_64-linux-gnu/11/libstdc++.so /opt/miniconda/lib/libstdc++.so.6

EOF

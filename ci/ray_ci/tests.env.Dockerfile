# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG RAY_CORE_IMAGE=scratch
ARG RAY_DASHBOARD_IMAGE=scratch

FROM "$RAY_CORE_IMAGE" AS ray_core
FROM "$RAY_DASHBOARD_IMAGE" AS ray_dashboard

FROM "$BASE_IMAGE"

ARG BUILD_TYPE
ARG BUILDKITE_CACHE_READONLY
ARG RAY_INSTALL_MASK=

# Disable C++ API/worker building by default on CI.
# To use C++ API/worker, set BUILD_TYPE to "multi-lang".
ENV RAY_DISABLE_EXTRA_CPP=1

RUN <<EOF
#!/bin/bash

set -euo pipefail

# For backward compatibility.
# base images should already have /rayci directory created.
if [[ ! -d /rayci ]]; then
  echo "WARNING: /rayci directory not found, creating it..."
  mkdir /rayci
fi

EOF

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

# Early exit cases
case "$BUILD_TYPE" in
  skip|ubsan)
    echo "Skipping building ray package"
    exit 0
    ;;
  clang|asan-clang|tsan-clang|cgroup)
    echo "--- Install LLVM dependencies (skip Ray installation)"
    bash ci/env/install-llvm-binaries.sh
    exit 0
    ;;
esac


# Helpers
install_redis() {
  local target_dir="$1"
  echo "--- Install Redis binaries"
  mkdir -p "${target_dir}"
  local arch="x86_64"
  if [[ "${HOSTTYPE}" =~ ^aarch64 ]]; then
    arch="arm64"
  fi
  curl -sSL "https://github.com/ray-project/redis/releases/download/7.2.3/redis-linux-${arch}.tar.gz" \
    | tar -xzf - -C "${target_dir}"
}

install_dashboard() {
  if [[ -e /opt/ray-dashboard/dashboard.tar.gz ]]; then
    echo "--- Extract prebuilt dashboard"
    mkdir -p python/ray/dashboard/client/build
    tar -xzf /opt/ray-dashboard/dashboard.tar.gz -C python/ray/dashboard/client/build
  else
    echo "--- Build dashboard from source"
    (cd python/ray/dashboard/client && npm ci && npm run build)
  fi
}

apply_install_mask() {
  local ray_dir="$1"
  if [[ "$RAY_INSTALL_MASK" =~ all-ray-libraries ]]; then
    echo "--- Apply install mask: $RAY_INSTALL_MASK"
    rm -rf "${ray_dir}/air"
    rm -rf "${ray_dir}/data"
    rm -rf "${ray_dir}/llm"
    rm -rf "${ray_dir}/rllib"
    rm -rf "${ray_dir}/serve"
    rm -rf "${ray_dir}/train"
    rm -rf "${ray_dir}/tune"
    rm -rf "${ray_dir}/workflow"
    # Source tree has a top-level rllib dir that symlinks into python/ray/rllib.
    rm -rf rllib
  fi
}

apply_install_mask "python/ray"
install_dashboard

# Dependencies are already installed in the base CI images.
# So we use --no-deps to avoid reinstalling them.
INSTALL_FLAGS=(--no-deps --force-reinstall -v)

# --- Install Ray per build type ---
case "$BUILD_TYPE" in
  optimized)
    if [[ -e /opt/ray-core/ray_pkg.zip && "$RAY_DISABLE_EXTRA_CPP" == "1" ]]; then
      echo "--- Extract prebuilt ray core"
      unzip -o -q /opt/ray-core/ray_pkg.zip -d python
      unzip -o -q /opt/ray-core/ray_py_proto.zip -d python
      install_redis "python/ray/core/src/ray/thirdparty/redis/src"
      echo "--- Install Ray (editable, skip Bazel)"
      RAY_INSTALL_JAVA=0 SKIP_BAZEL_BUILD=1 pip install "${INSTALL_FLAGS[@]}" -e python/
    else
      echo "--- Install Ray (full Bazel build)"
      pip install "${INSTALL_FLAGS[@]}" -e python/
    fi
    ;;
  debug)
    echo "--- Install Ray (debug build)"
    RAY_DEBUG_BUILD=debug pip install "${INSTALL_FLAGS[@]}" -e python/
    ;;
  asan)
    echo "--- Install Ray (asan build)"
    pip install "${INSTALL_FLAGS[@]}" -e python/
    bazel run $(./ci/run/bazel_export_options) --no//:jemalloc_flag //:gen_ray_pkg
    ;;
  multi-lang)
    echo "--- Install Ray (multi-lang build)"
    RAY_DISABLE_EXTRA_CPP=0 RAY_INSTALL_JAVA=1 pip install "${INSTALL_FLAGS[@]}" -e python/
    ;;
  java)
    echo "--- Install Ray (java build)"
    bash java/build-jar-multiplatform.sh linux
    RAY_INSTALL_JAVA=1 pip install "${INSTALL_FLAGS[@]}" -e python/
    ;;
  *)
    echo "--- Install Ray (full build)"
    pip install "${INSTALL_FLAGS[@]}" -e python/
    ;;
esac

# HACK: Ubuntu 24.04 system libstdc++ provides GLIBCXX_3.4.33 (GCC 14),
# but conda's bundled libstdc++ may not include all these symbols.
# When Ray is compiled locally (non-wheel build types), raylet links against
# the system libstdc++ and fails to load in the conda environment.
# Symlink the system library into the conda lib dir to fix this.
#
# This is NOT needed for wheel builds (manylinux wheels bundle compatible libs)
# but is harmless for them, so we apply it unconditionally for simplicity.
#
# Proper fix options:
#   - Always use prebuilt manylinux wheels (eliminates local compilation)
#   - Define a Bazel C++ toolchain independent of the system toolchain
#   - Downgrade system libstdc++ to match conda's version
if [[ -f /usr/lib/gcc/x86_64-linux-gnu/14/libstdc++.so ]]; then
  ln -sf /usr/lib/gcc/x86_64-linux-gnu/14/libstdc++.so /opt/miniforge/lib/libstdc++.so
  ln -sf /usr/lib/gcc/x86_64-linux-gnu/14/libstdc++.so /opt/miniforge/lib/libstdc++.so.6
elif [[ -f /usr/lib/gcc/aarch64-linux-gnu/14/libstdc++.so ]]; then
  ln -sf /usr/lib/gcc/aarch64-linux-gnu/14/libstdc++.so /opt/miniforge/lib/libstdc++.so
  ln -sf /usr/lib/gcc/aarch64-linux-gnu/14/libstdc++.so /opt/miniforge/lib/libstdc++.so.6
fi

EOF

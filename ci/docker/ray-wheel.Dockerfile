# syntax=docker/dockerfile:1.3-labs
#
# Ray Wheel Builder (Unified)
# ===========================
# Builds manylinux2014-compatible wheels using pre-built C++ artifacts from wanda cache.
#
# Build Types:
#   - ray wheel:     WHEEL_TYPE=ray (default) - Standard Ray Python wheel
#   - ray-cpp wheel: WHEEL_TYPE=cpp          - Ray C++ API wheel
#
# GLIBC Compatibility:
# --------------------
# manylinux2014 requires GLIBC <= 2.17 for broad Linux compatibility.
# The pre-built _raylet.so is compiled inside manylinux2014 with GLIBC 2.17.
#

ARG RAY_CORE_IMAGE
ARG RAY_CPP_CORE_IMAGE=scratch
ARG RAY_JAVA_IMAGE
ARG RAY_DASHBOARD_IMAGE
ARG MANYLINUX_VERSION
ARG HOSTTYPE

FROM ${RAY_CORE_IMAGE} AS ray-core
FROM ${RAY_CPP_CORE_IMAGE} AS ray-cpp-core
FROM ${RAY_JAVA_IMAGE} AS ray-java
FROM ${RAY_DASHBOARD_IMAGE} AS ray-dashboard

# Main build stage - manylinux2014 provides GLIBC 2.17
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

ARG PYTHON_VERSION=3.10
ARG BUILDKITE_COMMIT
ARG WHEEL_TYPE=ray

# Set environment variables for the build
# - BUILDKITE_COMMIT: Used for ray.__commit__. Defaults to "unknown" for local builds.
# - SKIP_BAZEL_BUILD=1: Skip bazel build, use pre-built artifacts from ray-core/ray-java/ray-dashboard
# - RAY_DISABLE_EXTRA_CPP: 1 for ray wheel only, 0 for ray-cpp wheel
# - WHEEL_TYPE: "ray" or "cpp" - determines which wheel to build
ENV BUILDKITE_COMMIT=${BUILDKITE_COMMIT:-unknown} \
    PYTHON_VERSION=${PYTHON_VERSION} \
    SKIP_BAZEL_BUILD=1 \
    WHEEL_TYPE=${WHEEL_TYPE}

WORKDIR /home/forge/ray

# Copy artifacts from all stages
COPY --from=ray-core /ray_pkg.zip /tmp/
COPY --from=ray-core /ray_py_proto.zip /tmp/
COPY --from=ray-java /ray_java_pkg.zip /tmp/
COPY --from=ray-dashboard /dashboard.tar.gz /tmp/

# Source files needed for wheel build
COPY --chown=2000:100 ci/build/build-manylinux-wheel.sh ci/build/
COPY --chown=2000:100 README.rst pyproject.toml ./
COPY --chown=2000:100 rllib/ rllib/
COPY --chown=2000:100 python/ python/

USER forge
# Note: ray-cpp-core may be "scratch" (empty) for ray-only builds
RUN --mount=from=ray-cpp-core,source=/,target=/ray-cpp-core,ro \
    <<'EOF'
#!/bin/bash
set -euo pipefail

PY_VERSION="${PYTHON_VERSION//./}"
PY_BIN="cp${PY_VERSION}-cp${PY_VERSION}"

# Verify required artifacts exist before unpacking
for f in /tmp/ray_pkg.zip /tmp/ray_py_proto.zip /tmp/ray_java_pkg.zip /tmp/dashboard.tar.gz; do
  [[ -f "$f" ]] || { echo "ERROR: missing artifact: $f"; exit 1; }
done

# Clean extraction dirs to avoid stale leftovers
rm -rf /tmp/ray_pkg /tmp/ray_java_pkg /tmp/ray_cpp_pkg
mkdir -p /tmp/ray_pkg /tmp/ray_java_pkg

# Unpack common pre-built artifacts
unzip -o /tmp/ray_pkg.zip -d /tmp/ray_pkg
unzip -o /tmp/ray_py_proto.zip -d python/
unzip -o /tmp/ray_java_pkg.zip -d /tmp/ray_java_pkg
mkdir -p python/ray/dashboard/client/build
tar -xzf /tmp/dashboard.tar.gz -C python/ray/dashboard/client/build/

# C++ core artifacts
cp -r /tmp/ray_pkg/ray/* python/ray/

# Java JARs
cp -r /tmp/ray_java_pkg/ray/* python/ray/

# Handle wheel type specific setup
if [[ "$WHEEL_TYPE" == "cpp" ]]; then
  # C++ API artifacts (headers, libs, examples)
  if [[ -f /ray-cpp-core/ray_cpp_pkg.zip ]]; then
    mkdir -p /tmp/ray_cpp_pkg
    unzip -o /ray-cpp-core/ray_cpp_pkg.zip -d /tmp/ray_cpp_pkg
    cp -r /tmp/ray_cpp_pkg/ray/cpp python/ray/
  else
    echo "ERROR: ray_cpp_pkg.zip not found for cpp wheel build"
    exit 1
  fi
  export RAY_DISABLE_EXTRA_CPP=0
else
  export RAY_DISABLE_EXTRA_CPP=1
fi

# Build wheels
./ci/build/build-manylinux-wheel.sh "$PY_BIN"

# Filter output based on wheel type
if [[ "$WHEEL_TYPE" == "cpp" ]]; then
  # Keep only ray-cpp wheel
  rm -f .whl/ray-[0-9]*.whl
fi

# Sanity check: ensure wheels exist
shopt -s nullglob
wheels=(.whl/*.whl)
if (( ${#wheels[@]} == 0 )); then
  echo "ERROR: No wheels produced in .whl/"
  ls -la .whl || true
  exit 1
fi

EOF

FROM scratch
COPY --from=builder /home/forge/ray/.whl/*.whl /

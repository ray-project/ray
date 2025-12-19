# syntax=docker/dockerfile:1.3-labs
#
# Ray C++ Wheel Builder
# =====================
# Builds the ray-cpp wheel using pre-built C++ artifacts from wanda cache.
#
# This wheel provides the Ray C++ API for building C++ applications with Ray.
# It requires all the same artifacts as the regular ray wheel, plus:
#   - ray_cpp_pkg.zip: C++ headers, libraries, and examples
#
# GLIBC Compatibility:
# --------------------
# Same requirements as ray-wheel - see GLIBC_COMPATIBILITY.md
# The ray_cpp_pkg.zip must be built on manylinux2014 (GLIBC 2.17).
#
# All ARGs for FROM statements must be declared at the top
ARG RAY_CORE_IMAGE
ARG RAY_CPP_CORE_IMAGE
ARG RAY_JAVA_IMAGE
ARG RAY_DASHBOARD_IMAGE
ARG MANYLINUX_VERSION
ARG HOSTTYPE

# Import cached artifacts from wanda as named stages
FROM ${RAY_CORE_IMAGE} AS ray-core
FROM ${RAY_CPP_CORE_IMAGE} AS ray-cpp-core
FROM ${RAY_JAVA_IMAGE} AS ray-java
FROM ${RAY_DASHBOARD_IMAGE} AS ray-dashboard

# Main build stage - manylinux2014 provides GLIBC 2.17
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

# Build args for wheel building
ARG PYTHON_VERSION=3.10
ARG BUILDKITE_COMMIT

# Set environment variables for the build
# BUILDKITE_COMMIT is used for ray.__commit__. Defaults to "unknown" for local builds.
# NOTE: We do NOT set RAY_DISABLE_EXTRA_CPP here - we want to build the cpp wheel
ENV BUILDKITE_COMMIT=${BUILDKITE_COMMIT:-unknown} \
    PYTHON_VERSION=${PYTHON_VERSION} \
    SKIP_BAZEL_BUILD=1

WORKDIR /home/forge/ray

# Copy pre-built artifacts from wanda-cached stages
COPY --from=ray-core /ray_pkg.zip /tmp/
COPY --from=ray-core /ray_py_proto.zip /tmp/
COPY --from=ray-cpp-core /ray_cpp_pkg.zip /tmp/
COPY --from=ray-java /ray_java_pkg.zip /tmp/
COPY --from=ray-dashboard /dashboard.tar.gz /tmp/

# Copy source files needed for wheel building
# Use --chown to set ownership during copy (avoids slow recursive chown)
# Order: least frequently changed first for better cache utilization
COPY --chown=2000:100 ci/build/build-manylinux-wheel.sh ci/build/
COPY --chown=2000:100 README.rst pyproject.toml ./
COPY --chown=2000:100 rllib/ rllib/
COPY --chown=2000:100 python/ python/

# Build the wheel using cached artifacts
USER forge
RUN <<EOF
#!/bin/bash
set -euo pipefail

PY_VERSION="${PYTHON_VERSION//./}"
PY_BIN="cp${PY_VERSION}-cp${PY_VERSION}"

# Unpack pre-built artifacts
# ray_pkg.zip structure: ray/_raylet.so, ray/core/src/ray/gcs/gcs_server, etc.
unzip -o /tmp/ray_pkg.zip -d /tmp/ray_pkg
unzip -o /tmp/ray_py_proto.zip -d python/
unzip -o /tmp/ray_cpp_pkg.zip -d /tmp/ray_cpp_pkg
tar -xzf /tmp/dashboard.tar.gz -C python/ray/dashboard/client/

# Copy C++ core artifacts to expected locations
# CRITICAL: The zip has 'ray/' at root, so we must copy ray/* (not /tmp/ray_pkg/*)
# to python/ray/. Wrong paths cause _raylet.so to be rebuilt with wrong GLIBC.
cp -r /tmp/ray_pkg/ray/* python/ray/

# Copy C++ API artifacts (headers, libs, examples) to python/ray/cpp/
# The ray_cpp_pkg.zip has 'ray/cpp/' structure, so unzip to python/
# This creates python/ray/cpp/include/, python/ray/cpp/lib/, etc.
cp -r /tmp/ray_cpp_pkg/ray/cpp python/ray/

# Build wheel (should skip C++ compilation since artifacts exist)
# This will build BOTH ray and ray-cpp wheels
./ci/build/build-manylinux-wheel.sh "$PY_BIN"

# Keep only the ray-cpp wheel, remove the regular ray wheel
rm -f .whl/ray-[0-9]*.whl
EOF

# Output wheel files (only ray-cpp wheel)
FROM scratch
COPY --from=builder /home/forge/ray/.whl/ray_cpp*.whl /

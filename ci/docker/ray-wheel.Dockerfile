# syntax=docker/dockerfile:1.3-labs
#
# Ray Wheel Builder
# =================
# Builds a manylinux2014-compatible wheel using pre-built C++ artifacts from wanda cache.
#
# GLIBC Compatibility (IMPORTANT):
# --------------------------------
# manylinux2014 requires GLIBC <= 2.17 for broad Linux compatibility.
# The pre-built _raylet.so from ray-core is compiled inside manylinux2014 with GLIBC 2.17.
#
# If _raylet.so is not found at python/ray/_raylet.so, Cython will rebuild it using
# the build environment's GLIBC, which may be newer and break compatibility.
#
# To verify wheel GLIBC requirements after build:
#   unzip -p ray-*.whl "ray/_raylet.so" > /tmp/raylet.so
#   objdump -p /tmp/raylet.so | grep GLIBC | sort -u
# The highest version should be GLIBC_2.17 or lower.
#
# All ARGs that are used in FROM statements must be declared at the top
# These are passed dynamically by wanda based on PYTHON_VERSION and ARCH_SUFFIX
ARG RAY_CORE_IMAGE
ARG RAY_JAVA_IMAGE
ARG RAY_DASHBOARD_IMAGE
ARG MANYLINUX_VERSION
ARG HOSTTYPE

# Import cached artifacts from wanda as named stages
FROM ${RAY_CORE_IMAGE} AS ray-core
FROM ${RAY_JAVA_IMAGE} AS ray-java
FROM ${RAY_DASHBOARD_IMAGE} AS ray-dashboard

# Main build stage - manylinux2014 provides GLIBC 2.17
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

# Build args for wheel building
ARG PYTHON_VERSION=3.10
ARG BUILDKITE_COMMIT

# Set environment variables for the build
# BUILDKITE_COMMIT is used for ray.__commit__. Defaults to "unknown" for local builds.
ENV BUILDKITE_COMMIT=${BUILDKITE_COMMIT:-unknown} \
    PYTHON_VERSION=${PYTHON_VERSION} \
    SKIP_BAZEL_BUILD=1 \
    RAY_DISABLE_EXTRA_CPP=1

WORKDIR /home/forge/ray

# Copy pre-built artifacts from wanda-cached stages
COPY --from=ray-core /ray_pkg.zip /tmp/
COPY --from=ray-core /ray_py_proto.zip /tmp/
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
tar -xzf /tmp/dashboard.tar.gz -C python/ray/dashboard/client/

# Copy C++ artifacts to expected locations
# CRITICAL: The zip has 'ray/' at root, so we must copy ray/* (not /tmp/ray_pkg/*)
# to python/ray/. Wrong paths cause _raylet.so to be rebuilt with wrong GLIBC.
cp -r /tmp/ray_pkg/ray/* python/ray/

# Build wheel (should skip C++ compilation since artifacts exist)
./ci/build/build-manylinux-wheel.sh "$PY_BIN"
EOF

# Output wheel files
FROM scratch
COPY --from=builder /home/forge/ray/.whl/*.whl /

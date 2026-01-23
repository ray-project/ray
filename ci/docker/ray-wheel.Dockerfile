# syntax=docker/dockerfile:1.3-labs
#
# Ray Wheel Builder
# =================
# Builds manylinux2014-compatible ray wheel using pre-built C++ artifacts from wanda cache.
#
# GLIBC Compatibility:
# --------------------
# manylinux2014 requires GLIBC <= 2.17 for broad Linux compatibility.
# The pre-built _raylet.so is compiled inside manylinux2014 with GLIBC 2.17.
#

ARG RAY_CORE_IMAGE
ARG RAY_JAVA_IMAGE
ARG RAY_DASHBOARD_IMAGE
ARG MANYLINUX_VERSION
ARG HOSTTYPE

FROM ${RAY_CORE_IMAGE} AS ray-core
FROM ${RAY_JAVA_IMAGE} AS ray-java
FROM ${RAY_DASHBOARD_IMAGE} AS ray-dashboard

# Main build stage - manylinux2014 provides GLIBC 2.17
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

ARG PYTHON_VERSION=3.10
ARG BUILDKITE_COMMIT

WORKDIR /home/forge/ray

# Copy artifacts from all stages
COPY --from=ray-core /ray_pkg.zip /tmp/
COPY --from=ray-core /ray_py_proto.zip /tmp/
COPY --from=ray-java /ray_java_pkg.zip /tmp/
COPY --from=ray-dashboard /dashboard.tar.gz /tmp/

# Source files needed for wheel build
COPY --chown=forge ci/build/build-manylinux-wheel.sh ci/build/
COPY --chown=forge README.rst pyproject.toml ./
COPY --chown=forge rllib/ rllib/
COPY --chown=forge python/ python/

USER forge
# - BUILDKITE_COMMIT: Used for ray.__commit__. Defaults to "unknown" for local builds.
ENV PYTHON_VERSION=${PYTHON_VERSION} \
    BUILDKITE_COMMIT=${BUILDKITE_COMMIT:-unknown}
RUN <<'EOF'
#!/bin/bash
set -euo pipefail

# Clean extraction dirs to avoid stale leftovers
rm -rf /tmp/ray_pkg /tmp/ray_java_pkg
mkdir -p /tmp/ray_pkg /tmp/ray_java_pkg

# Unpack pre-built artifacts
unzip -o /tmp/ray_pkg.zip -d /tmp/ray_pkg
unzip -o /tmp/ray_py_proto.zip -d python/
unzip -o /tmp/ray_java_pkg.zip -d /tmp/ray_java_pkg
mkdir -p python/ray/dashboard/client/build
tar -xzf /tmp/dashboard.tar.gz -C python/ray/dashboard/client/build/

# C++ core artifacts
cp -r /tmp/ray_pkg/ray/* python/ray/

# Java JARs
cp -r /tmp/ray_java_pkg/ray/* python/ray/

# Build ray wheel
PY_VERSION="${PYTHON_VERSION//./}"
PY_BIN="cp${PY_VERSION}-cp${PY_VERSION}"
SKIP_BAZEL_BUILD=1 RAY_DISABLE_EXTRA_CPP=1 \
./ci/build/build-manylinux-wheel.sh "$PY_BIN"

# Sanity check: ensure wheels exist
if [[ ! -d .whl ]]; then
  echo "ERROR: .whl directory not created"
  exit 1
fi
wheels=($(find .whl -maxdepth 1 -name '*.whl'))
if (( ${#wheels[@]} == 0 )); then
  echo "ERROR: No wheels produced in .whl/"
  ls -la .whl
  exit 1
fi

EOF

FROM scratch
COPY --from=builder /home/forge/ray/.whl/*.whl /

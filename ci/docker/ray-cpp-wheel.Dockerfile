# syntax=docker/dockerfile:1.3-labs
#
# Ray C++ Wheel Builder
# =====================
# Builds manylinux2014-compatible ray-cpp wheel using pre-built C++ artifacts from wanda cache.
#
# This is a minimal Dockerfile for ray-cpp wheel builds only.
# It copies only the files needed for the cpp wheel, reducing build context size.

ARG RAY_CORE_IMAGE
ARG RAY_CPP_CORE_IMAGE
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

ARG BUILDKITE_COMMIT

WORKDIR /home/forge/ray

# Copy artifacts from all stages
COPY --from=ray-core /ray_pkg.zip /tmp/
COPY --from=ray-core /ray_py_proto.zip /tmp/
COPY --from=ray-java /ray_java_pkg.zip /tmp/
COPY --from=ray-dashboard /dashboard.tar.gz /tmp/

# Minimal source files needed for cpp wheel build
COPY --chown=forge ci/build/build-ray-cpp-wheel.sh ci/build/
COPY --chown=forge README.rst pyproject.toml ./
COPY --chown=forge python/setup.py python/
COPY --chown=forge python/LICENSE.txt python/
COPY --chown=forge python/MANIFEST.in python/
COPY --chown=forge python/ray/_version.py python/ray/

USER forge
ENV BUILDKITE_COMMIT=${BUILDKITE_COMMIT:-unknown}
RUN --mount=from=ray-cpp-core,source=/,target=/ray-cpp-core,ro \
    <<'EOF'
#!/bin/bash
set -euo pipefail

# Clean extraction dirs to avoid stale leftovers
rm -rf /tmp/ray_pkg /tmp/ray_java_pkg /tmp/ray_cpp_pkg
mkdir -p /tmp/ray_pkg /tmp/ray_java_pkg /tmp/ray_cpp_pkg

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

# C++ API artifacts (headers, libs, examples)
unzip -o /ray-cpp-core/ray_cpp_pkg.zip -d /tmp/ray_cpp_pkg
cp -r /tmp/ray_cpp_pkg/ray/cpp python/ray/

# Build Python-agnostic cpp wheel
./ci/build/build-ray-cpp-wheel.sh

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

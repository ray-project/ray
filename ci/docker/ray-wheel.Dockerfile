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
ARG RAY_JAVA_IMAGE=scratch
ARG RAY_DASHBOARD_IMAGE=scratch
ARG MANYLINUX_IMAGE

FROM ${RAY_CORE_IMAGE} AS ray-core
FROM ${RAY_JAVA_IMAGE} AS ray-java
FROM ${RAY_DASHBOARD_IMAGE} AS ray-dashboard

# Main build stage - manylinux2014 provides GLIBC 2.17
FROM ${MANYLINUX_IMAGE} AS builder

# Where pip resolves from while building the wheel. This stage builds FROM the upstream
# manylinux image, so it inherits nothing from the CI image roots, and a docker build
# cannot see an index configured in the step's environment -- BuildKit RUN steps inherit
# nothing from it. So it arrives as a build arg, which wanda resolves from
# RAYCI_IMAGE_PIP_INDEX_URL in the job environment. Empty outside CI, and then this is
# the index pip would have used anyway.
ARG RAYCI_IMAGE_PIP_INDEX_URL=""
ENV PIP_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}

# pip refuses a plain-HTTP index unless the host is named as trusted, with loopback the
# one exemption -- and this address is a name, not loopback. The refusal is silent: the
# index is dropped and the install fails with "from versions: none" rather than a
# connection error (release 104844, cython==3.0.12 in the wheel build). Arrives the same
# way as the index above and is empty outside CI, where the index is public PyPI over
# HTTPS and there is nothing to trust.
ARG RAYCI_IMAGE_PIP_TRUSTED_HOST=""
ENV PIP_TRUSTED_HOST=${RAYCI_IMAGE_PIP_TRUSTED_HOST}

ARG PYTHON_VERSION=3.10
ARG BUILDKITE_COMMIT

WORKDIR /home/forge/ray

# Copy artifacts from all stages
COPY --from=ray-core /ray_pkg.zip /tmp/
COPY --from=ray-core /ray_py_proto.zip /tmp/

# Source files needed for wheel build
COPY --chown=forge ci/build/build-manylinux-wheel.sh ci/build/
COPY --chown=forge README.rst pyproject.toml ./
COPY --chown=forge rllib/ rllib/
COPY --chown=forge python/ python/

USER forge
# - BUILDKITE_COMMIT: Used for ray.__commit__. Defaults to "unknown" for local builds.
ENV PYTHON_VERSION=${PYTHON_VERSION} \
    BUILDKITE_COMMIT=${BUILDKITE_COMMIT:-unknown}
RUN --mount=from=ray-java,target=/mnt/java \
    --mount=from=ray-dashboard,target=/mnt/dashboard \
    <<'EOF'
#!/bin/bash
set -euo pipefail

# Clean extraction dirs to avoid stale leftovers
rm -rf /tmp/ray_pkg
mkdir -p /tmp/ray_pkg

# Unpack pre-built artifacts
unzip -o /tmp/ray_pkg.zip -d /tmp/ray_pkg
unzip -o /tmp/ray_py_proto.zip -d python/

# Dashboard (optional)
if [[ -f /mnt/dashboard/dashboard.tar.gz ]]; then
    mkdir -p python/ray/dashboard/client/build
    tar -xzf /mnt/dashboard/dashboard.tar.gz -C python/ray/dashboard/client/build/
fi

# C++ core artifacts
cp -r /tmp/ray_pkg/ray/* python/ray/

# Java JARs (optional)
if [[ -f /mnt/java/ray_java_pkg.zip ]]; then
    mkdir -p /tmp/ray_java_pkg
    unzip -o /mnt/java/ray_java_pkg.zip -d /tmp/ray_java_pkg
    cp -r /tmp/ray_java_pkg/ray/* python/ray/
fi

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
COPY --from=builder /home/forge/ray/.whl/*.whl /opt/artifacts/

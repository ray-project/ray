# syntax=docker/dockerfile:1.3-labs

# NOTE(andrew-anyscale): This Dockerfile is used to retag the manylinux image to
# a new tag with Wanda. This is kept as-is until we can rework LinuxContainer to
# use a hardcoded image tag not hosted in ECR. See:
# https://github.com/ray-project/ray/blob/6ab10189b0f6506158bac76437a97b28c2643155/ci/ray_ci/builder_container.py#L16
# https://github.com/ray-project/ray/blob/master/ci/ray_ci/container.py#L57C49-L57C59

ARG MANYLINUX_VERSION
ARG HOSTTYPE
ARG BUILDKITE_BAZEL_CACHE_URL
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE}

# Install Python 3.14 using uv (not yet available in the base manylinux2014 image)
# TODO(python314): Remove this once rayproject/manylinux2014 is rebuilt with Python 3.14
USER root
RUN <<EOF
#!/bin/bash
set -euo pipefail

# Install Python 3.14 using uv (already installed in base image)
uv python install 3.14

# Create the /opt/python/cp314-cp314 directory structure expected by build scripts
mkdir -p /opt/python/cp314-cp314/bin

# Find the uv-installed Python 3.14 and create symlinks
PYTHON314_PATH=$(uv python find 3.14)
PYTHON314_DIR=$(dirname $(dirname "$PYTHON314_PATH"))

# Create symlinks for python3, python, and pip
ln -sf "$PYTHON314_PATH" /opt/python/cp314-cp314/bin/python3
ln -sf "$PYTHON314_PATH" /opt/python/cp314-cp314/bin/python

# Install pip using ensurepip
"$PYTHON314_PATH" -m ensurepip --upgrade
ln -sf "$PYTHON314_DIR/bin/pip3" /opt/python/cp314-cp314/bin/pip3
ln -sf "$PYTHON314_DIR/bin/pip3" /opt/python/cp314-cp314/bin/pip

# Verify installation
/opt/python/cp314-cp314/bin/python3 --version
/opt/python/cp314-cp314/bin/pip --version

EOF
USER forge

# Still keep bazelrc updates to allow BUILDKITE_BAZEL_CACHE_URL to be used.
RUN <<EOF
#!/bin/bash

set -euo pipefail

{
  echo "build --config=ci"
  echo "build --announce_rc"
  if [[ "${BUILDKITE_BAZEL_CACHE_URL:-}" != "" ]]; then
    echo "build:ci --remote_cache=${BUILDKITE_BAZEL_CACHE_URL:-}"
  fi
} > "$HOME"/.bazelrc

EOF

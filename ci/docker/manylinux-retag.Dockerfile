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

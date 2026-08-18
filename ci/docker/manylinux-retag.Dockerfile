# syntax=docker/dockerfile:1.3-labs

# NOTE(andrew-anyscale): This Dockerfile is used to retag the manylinux image to
# a new tag with Wanda. This is kept as-is until we can rework LinuxContainer to
# use a hardcoded image tag not hosted in ECR. See:
# https://github.com/ray-project/ray/blob/6ab10189b0f6506158bac76437a97b28c2643155/ci/ray_ci/builder_container.py#L16
# https://github.com/ray-project/ray/blob/master/ci/ray_ci/container.py#L57C49-L57C59

ARG MANYLINUX_VERSION
ARG HOSTTYPE
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE}

ARG BUILDKITE_BAZEL_CACHE_URL
ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}

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

# Package index configuration for CI, sourced by the login shell each step runs.
# No-op unless the agent wrote the matching files into the checkout.
#
# The base image already runs as forge (crane config reports User "forge"), and
# /etc/profile.d is root-owned, so this has to step up and back down again.
USER root
RUN \
  --mount=type=bind,source=ci/docker/rayci-codeartifact-profile.sh,target=rayci-codeartifact-profile.sh \
<<EOF
#!/bin/bash

set -euo pipefail

install -D -m 0644 rayci-codeartifact-profile.sh \
  /etc/profile.d/zz-rayci-codeartifact.sh

EOF

USER forge

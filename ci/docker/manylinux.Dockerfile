# syntax=docker/dockerfile:1.3-labs

ARG HOSTTYPE
FROM quay.io/pypa/manylinux2014_${HOSTTYPE}:2024-07-02-9ac04ee

ARG BUILDKITE_BAZEL_CACHE_URL
ARG RAYCI_DISABLE_JAVA=false

# uid needs to be synced with forge.Dockerfile
ARG FORGE_UID=2000

ENV BUILD_JAR=1
ENV RAYCI_DISABLE_JAVA=$RAYCI_DISABLE_JAVA
ENV RAY_INSTALL_JAVA=1
ENV BUILDKITE_BAZEL_CACHE_URL=$BUILDKITE_BAZEL_CACHE_URL

RUN yum -y install sudo

RUN curl -LsSf https://astral.sh/uv/0.8.17/install.sh | \
    env UV_INSTALL_DIR=/usr/local/bin sh

RUN <<EOF
#!/bin/bash

set -euo pipefail

useradd -m -u "$FORGE_UID" -g users -d /home/forge forge
echo 'forge ALL=NOPASSWD: ALL' >> /etc/sudoers

EOF

COPY ci/build/build-manylinux-forge.sh /tmp/build-manylinux-forge.sh

RUN ./tmp/build-manylinux-forge.sh

USER forge
ENV HOME=/home/forge

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

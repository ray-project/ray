# syntax=docker/dockerfile:1.3-labs

ARG HOSTTYPE
FROM quay.io/pypa/manylinux2014_${HOSTTYPE}:2026.01.02-1

ARG BUILDKITE_BAZEL_CACHE_URL
ARG RAYCI_DISABLE_JAVA=false

# uid needs to be synced with forge.Dockerfile
ARG FORGE_UID=2000

ENV BUILD_JAR=1

# Where pip and uv resolve from while building this image. Docker builds cannot see an
# index configured in the CI step's environment -- BuildKit RUN steps inherit nothing
# from it -- so it arrives as a build arg, which wanda resolves from
# RAYCI_IMAGE_PIP_INDEX_URL in the job environment.
#
# Empty for anyone building these images outside CI, and then this is exactly the index
# pip would have used anyway, so an external build behaves as it does today.
ARG RAYCI_IMAGE_PIP_INDEX_URL=""
ENV PIP_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}
ENV UV_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}

ENV RAYCI_DISABLE_JAVA=$RAYCI_DISABLE_JAVA
ENV RAY_INSTALL_JAVA=1
ENV BUILDKITE_BAZEL_CACHE_URL=$BUILDKITE_BAZEL_CACHE_URL

# See ci/docker/base.test.Dockerfile: retry transient files.pythonhosted.org failures. This
# is the image the wheel builds run in, where the manylinux interpreters' own pip installs
# build dependencies such as cython and setuptools straight from PyPI.
ENV PIP_RETRIES=9
ENV UV_HTTP_RETRIES=9

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

# last kick: 2025-10-08

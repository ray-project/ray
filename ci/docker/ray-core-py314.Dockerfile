# syntax=docker/dockerfile:1.3-labs
# Python 3.14 specific Dockerfile - builds directly from PyPA manylinux base image

ARG ARCH_SUFFIX=
ARG HOSTTYPE=x86_64
FROM quay.io/pypa/manylinux2014_${HOSTTYPE}:2025.12.19-1 AS base

ARG BUILDKITE_BAZEL_CACHE_URL

# uid needs to be synced with forge.Dockerfile
ARG FORGE_UID=2000

ENV BUILD_JAR=1
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

FROM base AS builder

ARG PYTHON_VERSION=3.14
ARG BUILDKITE_BAZEL_CACHE_URL
ARG BUILDKITE_CACHE_READONLY

WORKDIR /home/forge/ray

COPY --chown=forge:users . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

PY_CODE="${PYTHON_VERSION//./}"
PY_BIN="cp${PY_CODE}-cp${PY_CODE}"

export RAY_BUILD_ENV="manylinux_py${PY_BIN}"

sudo ln -sf "/opt/python/${PY_BIN}/bin/python3" /usr/local/bin/python3
sudo ln -sf /usr/local/bin/python3 /usr/local/bin/python

if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
  echo "build --remote_upload_local_results=false" >> "$HOME/.bazelrc"
fi

bazelisk build --config=ci //:ray_pkg_zip //:ray_py_proto_zip

cp bazel-bin/ray_pkg.zip /home/forge/ray_pkg.zip
cp bazel-bin/ray_py_proto.zip /home/forge/ray_py_proto.zip

EOF

FROM scratch

COPY --from=builder /home/forge/ray_pkg.zip /home/forge/ray_py_proto.zip /

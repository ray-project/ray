# syntax=docker/dockerfile:1.3-labs
ARG ARCH_SUFFIX
ARG HOSTTYPE=x86_64
ARG MANYLINUX_VERSION
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

ARG BUILDKITE_BAZEL_CACHE_URL
ARG BUILDKITE_CACHE_READONLY
ARG HOSTTYPE
ARG IS_LOCAL_BUILD=false
ARG CACHE_DIR=/opt/cache

ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}
ENV BUILDKITE_CACHE_READONLY=${BUILDKITE_CACHE_READONLY}
ENV IS_LOCAL_BUILD=${IS_LOCAL_BUILD}
ENV CACHE_DIR=${CACHE_DIR}
ENV DOWNLOAD_CACHE=${CACHE_DIR}/downloads
ENV BAZEL_CACHE=${CACHE_DIR}/bazel

WORKDIR /home/forge/ray

COPY . .

RUN --mount=type=cache,target=${DOWNLOAD_CACHE},uid=2000,gid=100,id=ray-downloads-${HOSTTYPE} \
    --mount=type=cache,target=${BAZEL_CACHE},uid=2000,gid=100,id=ray-bazel-${HOSTTYPE} \
    <<'EOF'
#!/bin/bash
set -euo pipefail

export BAZELISK_HOME=$DOWNLOAD_CACHE/bazelisk
export COURSIER_CACHE=$DOWNLOAD_CACHE/coursier
REPOSITORY_CACHE=$DOWNLOAD_CACHE/repo

export RAY_BUILD_ENV="manylinux"

BAZEL_CACHE_ARGS=""
if [[ -z "${BUILDKITE_BAZEL_CACHE_URL:-}" ]]; then
  # Disable remote cache for local builds (no credentials)
  BAZEL_CACHE_ARGS="--remote_cache="
elif [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
  # Read-only mode: disable uploads only
  BAZEL_CACHE_ARGS="--remote_upload_local_results=false"
fi

BAZEL_RESOURCE_FLAGS=""
if [[ "$IS_LOCAL_BUILD" == "true" ]]; then
  BAZEL_RESOURCE_FLAGS=$(python3 "$HOME/ray/ci/build/container_resource_utils.py")
fi

bazelisk --output_base=$BAZEL_CACHE run --config=ci \
    --repository_cache=$REPOSITORY_CACHE \
    $BAZEL_CACHE_ARGS \
    $BAZEL_RESOURCE_FLAGS \
    //java:gen_ray_java_pkg

cp bazel-bin/java/ray_java_pkg.zip /home/forge/ray_java_pkg.zip

EOF

FROM scratch

COPY --from=builder /home/forge/ray_java_pkg.zip /

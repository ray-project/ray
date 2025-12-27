# syntax=docker/dockerfile:1.3-labs
ARG HOSTTYPE=x86_64
ARG ARCH_SUFFIX
ARG MANYLINUX_VERSION
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

ARG BUILDKITE_BAZEL_CACHE_URL
ARG BUILDKITE_CACHE_READONLY
ARG HOSTTYPE

WORKDIR /home/forge/ray

COPY . .

# Mounting cache dir for faster local rebuilds (architecture-specific to avoid toolchain conflicts)
RUN --mount=type=cache,target=/home/forge/.cache,uid=2000,gid=100,id=bazel-cache-${HOSTTYPE}-java \
    <<EOF
#!/bin/bash

set -euo pipefail

export RAY_BUILD_ENV="manylinux"

echo "build:ci --remote_cache=${BUILDKITE_BAZEL_CACHE_URL:-}" >> "$HOME/.bazelrc"
if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
  echo "build --remote_upload_local_results=false" >> "$HOME/.bazelrc"
fi

echo "build --repository_cache=/home/forge/.cache/bazel-repo" >> "$HOME/.bazelrc"
echo "build --experimental_repository_cache_hardlinks" >> "$HOME/.bazelrc"

source "$HOME/ray/ci/build/local-build-utils.sh"
BAZEL_FLAGS="$(bazel_container_resource_flags)"

bazelisk run --config=ci ${BAZEL_FLAGS} //java:gen_ray_java_pkg

cp bazel-bin/java/ray_java_pkg.zip /home/forge/ray_java_pkg.zip

EOF

FROM scratch

COPY --from=builder /home/forge/ray_java_pkg.zip /

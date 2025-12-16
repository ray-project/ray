# syntax=docker/dockerfile:1.3-labs
ARG ARCH_SUFFIX
FROM rayproject/manylinux2014:251216.3835fc5-jdk-$HOSTTYPE AS builder

ARG BUILDKITE_BAZEL_CACHE_URL
ARG BUILDKITE_CACHE_READONLY

WORKDIR /home/forge/ray

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

export RAY_BUILD_ENV="manylinux"

if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
  echo "build --remote_upload_local_results=false" >> "$HOME/.bazelrc"
fi

bazelisk run --config=ci //java:gen_ray_java_pkg

cp bazel-bin/java/ray_java_pkg.zip /home/forge/ray_java_pkg.zip

EOF

FROM scratch

COPY --from=builder /home/forge/ray_java_pkg.zip /

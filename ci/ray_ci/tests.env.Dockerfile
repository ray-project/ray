# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG TEST_ENVIRONMENT_SCRIPT

# ray tests require running as root
USER root
RUN mkdir /ray
WORKDIR /ray
COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

{
  echo "build --config=ci"
  echo "build --announce_rc"
  echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}"
} > ~/.bazelrc

EOF

# build dashboard
RUN cd dashboard/client && npm ci && npm run build

# install ray
RUN RAY_INSTALL_JAVA=1 pip install -v -e python/

COPY "$TEST_ENVIRONMENT_SCRIPT" /tmp/post_build_script.sh
RUN /tmp/post_build_script.sh

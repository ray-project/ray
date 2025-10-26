#!/bin/bash
set -exuo pipefail

# Do not upload results to remote cache for pull requests
if [[ "${BUILDKITE_CACHE_READONLY:-}" == "true" ]]; then
  echo "build --remote_upload_local_results=false" >> ~/.bazelrc
fi

# Build ray java
if [[ "${RAY_INSTALL_JAVA}" == "1" ]]; then
  bazel build //java:ray_java_pkg
fi

export PATH="/usr/local/node/bin:$PATH"

# Build ray dashboard
(
  cd python/ray/dashboard/client
  npm ci
  npm run build
)

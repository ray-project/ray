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

# The CI mirror's hosted PyPI index, so steps that run in this image resolve
# through the package mirror like the ones that run in forge. This is not a
# cosmetic gap: `build: raydepsets: compile all dependencies` runs here, and it is
# the step that fails on files.pythonhosted.org 502s (pypi/support#11895) because
# uv had no index configured and went straight to the origin.
# ci/pypi_proxy_profile.sh probes and decides per step; the bazel downloader
# helper lives beside it in /etc/rayci because the hook runs at shell start,
# before any checkout exists.
RUN \
  --mount=type=bind,source=ci/pypi_proxy_profile.sh,target=pypi_proxy_profile.sh \
  --mount=type=bind,source=ci/bazel_mirror_downloader.sh,target=bazel_mirror_downloader.sh \
<<EOF
#!/bin/bash

set -euo pipefail

# Under sudo because this image, unlike forge, has already switched to USER forge
# by the time we get here, and these land in /etc. The base grants forge
# passwordless sudo for exactly this.
sudo mkdir -p /etc/rayci
sudo cp bazel_mirror_downloader.sh /etc/rayci/bazel_mirror_downloader.sh
sudo cp pypi_proxy_profile.sh /etc/profile.d/zz-rayci-pypi-proxy.sh
sudo chmod 0644 /etc/profile.d/zz-rayci-pypi-proxy.sh /etc/rayci/bazel_mirror_downloader.sh
EOF

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

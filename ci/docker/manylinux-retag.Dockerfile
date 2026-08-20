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

# The PyPI index proxy, so steps that run in this image resolve through the CI
# package mirror like the ones that run in forge. This is not a cosmetic gap:
# `build: raydepsets: compile all dependencies` runs here, and it is the step that
# fails on files.pythonhosted.org 502s (pypi/support#11895) because uv had no index
# configured and went straight to the origin.
#
# manylinux ships a set of interpreters under /opt/python; pick the newest that
# satisfies the proxy's >=3.11 requirement rather than naming one, since the set
# moves with the base image. The image's default python is untouched.
RUN \
  --mount=type=bind,source=ci/pypi_index_proxy.py,target=pypi_index_proxy.py \
  --mount=type=bind,source=ci/pypi_proxy_profile.sh,target=pypi_proxy_profile.sh \
  --mount=type=bind,source=ci/install_pypi_proxy.sh,target=install_pypi_proxy.sh \
  --mount=type=bind,source=ci/bazel_mirror_downloader.sh,target=bazel_mirror_downloader.sh \
<<EOF
#!/bin/bash

set -euo pipefail

PROXY_PYTHON=""
for candidate in /opt/python/cp313-cp313/bin/python \
                 /opt/python/cp312-cp312/bin/python \
                 /opt/python/cp311-cp311/bin/python; do
  if [[ -x "${candidate}" ]]; then
    PROXY_PYTHON="${candidate}"
    break
  fi
done

if [[ -z "${PROXY_PYTHON}" ]]; then
  echo "no python >= 3.11 under /opt/python; cannot install the index proxy" >&2
  ls -1 /opt/python >&2 || true
  exit 1
fi

echo "installing the index proxy with ${PROXY_PYTHON}"

# Under sudo because this image, unlike forge, has already switched to USER forge
# by the time we get here, and the installer writes to /opt/pypiproxy and
# /etc/profile.d. The base grants forge passwordless sudo for exactly this.
sudo bash install_pypi_proxy.sh "${PROXY_PYTHON}"
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

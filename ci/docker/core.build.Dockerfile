ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON=3.10
ARG BUILD_VARIANT=build
ARG PYTHON_DEPSET=python/deplocks/ci/core-${BUILD_VARIANT}-ci_depset_py${PYTHON}.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

uv pip uninstall --system ray

EOF

# nvidia-container-toolkit-base (nvidia-ctk), needed by
# ray.experimental.sandbox's real GPU/CDI tests (gVisor --nvproxy; see
# .buildkite/core.rayci.yml's "core: sandbox gpu tests" job).
RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -qq
apt-get install -y -qq gnupg
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
  | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  > /etc/apt/sources.list.d/nvidia-container-toolkit.list
apt-get update -qq
apt-get install -y -qq nvidia-container-toolkit-base
rm -rf /var/lib/apt/lists/*

EOF

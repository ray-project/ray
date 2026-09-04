# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG PYTHON_VERSION=3.10
ARG IMAGE_TYPE="ray"
ARG PIP_REQUIREMENTS="python/deplocks/base_extra_testdeps/${IMAGE_TYPE}-base_extra_testdeps_py${PYTHON_VERSION}.lock"

# Where pip and uv resolve from while building this image. Docker builds cannot see an
# index configured in the CI step's environment -- BuildKit RUN steps inherit nothing
# from it -- so it arrives as a build arg, which wanda resolves from
# RAYCI_IMAGE_PIP_INDEX_URL in the job environment.
#
# Empty for anyone building this image outside CI, and then this is exactly the index
# pip would have used anyway. This one carries the release tests' extra dependencies,
# so a failed fetch here costs a nightly rather than one job.
#
# ARG rather than ENV on purpose: this image runs on Anyscale clusters outside the
# CI VPCs, where a persisted CI index URL can never resolve, so the value must not
# outlive the build.
ARG RAYCI_IMAGE_PIP_INDEX_URL=""
ARG PIP_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}
ARG UV_INDEX_URL=${RAYCI_IMAGE_PIP_INDEX_URL:-https://pypi.org/simple}

COPY "$PIP_REQUIREMENTS" extra-test-requirements.txt

# Make PyPI fetches survive a transient files.pythonhosted.org failure. Declared before
# the RUN below so the image build itself picks them up, and kept in the image so the
# byod_*.sh scripts that pip install at cluster startup inherit them too. pip reads PIP_*
# only when it is not run with --isolated, which is the case for every pip invocation on
# this path. Note that retrying an HTTP 502 specifically needs pip >= 24.0, which is where
# 502 was added to urllib3's status_forcelist; on older pip these still cover connection
# errors and 500/503. uv defaults to only 3 retries and has been seen to exhaust them on a
# 502, hence UV_HTTP_RETRIES; older uv releases ignore the variable rather than erroring.
ENV \
  PIP_RETRIES=9 \
  UV_HTTP_RETRIES=9

RUN <<EOF
#!/bin/bash

set -euo pipefail

APT_PKGS=(
    apt-transport-https
    ca-certificates
    htop
    libaio1
    libgl1-mesa-glx
    libglfw3
    libjemalloc-dev
    libosmesa6-dev
    lsb-release
)

sudo apt-get update -y
sudo apt-get install -y --no-install-recommends "${APT_PKGS[@]}"
sudo apt-get autoclean
sudo rm -rf /etc/apt/sources.list.d/*

sudo mkdir -p /etc/apt/keyrings
curl -sLS --retry 5 --retry-delay 2 \
  https://packages.microsoft.com/keys/microsoft.asc |
  gpg --dearmor | sudo tee /etc/apt/keyrings/microsoft.gpg > /dev/null
sudo chmod go+r /etc/apt/keyrings/microsoft.gpg

AZ_VER=2.72.0
AZ_DIST="$(lsb_release -cs)"
echo "Types: deb
URIs: https://packages.microsoft.com/repos/azure-cli/
Suites: ${AZ_DIST}
Components: main
Architectures: $(dpkg --print-architecture)
Signed-by: /etc/apt/keyrings/microsoft.gpg" | sudo tee /etc/apt/sources.list.d/azure-cli.sources

sudo apt-get update -y
sudo apt-get install -y azure-cli="${AZ_VER}"-1~"${AZ_DIST}"

git clone --branch=4.2.0 --depth=1 https://github.com/wg/wrk.git /tmp/wrk
make -C /tmp/wrk -j
sudo cp /tmp/wrk/wrk /usr/local/bin/wrk
rm -rf /tmp/wrk

"$HOME/anaconda3/bin/pip" install --no-cache-dir -r extra-test-requirements.txt

EOF

# RAY_BACKEND_LOG_JSON=1
#   Uses JSON structured logging.
#
# RAY_DATA_LOG_INTERNAL_STACK_TRACE_TO_STDOUT=1
#   Logs the full stack trace from Ray Data in case of exception,
#   which is useful for debugging failures.
#
# RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE=1
#   To make ray data compatible across multiple pyarrow versions.
ENV \
  RAY_BACKEND_LOG_JSON=1 \
  RAY_DATA_LOG_INTERNAL_STACK_TRACE_TO_STDOUT=1 \
  RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE=1

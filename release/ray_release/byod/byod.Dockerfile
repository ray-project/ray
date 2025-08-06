# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG PIP_REQUIREMENTS

RUN <<EOF
#!/bin/bash

set -euo pipefail

DEBIAN_REQUIREMENTS=(
    apt-transport-https
    ca-certificates
    curl
    htop
    gnupg
    google-cloud-sdk
    libaio1
    libgl1-mesa-glx
    libglfw3
    libjemalloc-dev
    libosmesa6-dev
    patchelf
    unzip
    zip
)

sudo apt-get update -y
sudo apt-get install -y --no-install-recommends "${DEBIAN_REQUIREMENTS[@]}"
sudo apt-get autoclean

rm -rf /tmp/wrk
git clone --branch 4.2.0 https://github.com/wg/wrk.git /tmp/wrk
make -C /tmp/wrk -j
sudo cp /tmp/wrk/wrk /usr/local/bin/wrk

EOF

COPY "$PIP_REQUIREMENTS" .
RUN "$HOME"/anaconda3/bin/pip install --no-cache-dir -r "${PIP_REQUIREMENTS}"

# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG PIP_REQUIREMENTS

COPY "$PIP_REQUIREMENTS" extra-test-requirements.txt

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
    patchelf
)

sudo apt-get update -y
sudo apt-get install -y --no-install-recommends "${APT_PKGS[@]}"
sudo apt-get autoclean
sudo rm -rf /etc/apt/sources.list.d/*

git clone --branch=4.2.0 --depth=1 https://github.com/wg/wrk.git /tmp/wrk
make -C /tmp/wrk -j
sudo cp /tmp/wrk/wrk /usr/local/bin/wrk
rm -rf /tmp/wrk

"$HOME/anaconda3/bin/pip" install --no-cache-dir -r extra-test-requirements.txt

EOF

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
    lsb-release
    patchelf
)

sudo apt-get update -y
sudo apt-get install -y --no-install-recommends "${APT_PKGS[@]}"
sudo apt-get autoclean
sudo rm -rf /etc/apt/sources.list.d/*

sudo mkdir -p /etc/apt/keyrings
curl -sLS https://packages.microsoft.com/keys/microsoft.asc |
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

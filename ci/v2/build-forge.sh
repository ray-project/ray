#!/bin/bash

set -euo pipefail

# Add docker APT repo
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list >/dev/null


apt-get update -y
apt-get upgrade -y
apt-get install -y docker-ce-cli awscli

export DOCKER_BUILDKIT=1

echo "--- Build forge"

tar --mtime="UTC 2020-01-01" -c -f - \
    ci/v2/forge/Dockerfile \
    | docker build --progress=plain -t forge \
        -f ci/v2/forge/Dockerfile -

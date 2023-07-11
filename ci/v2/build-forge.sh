#!/bin/bash

set -euo pipefail

export DEBIAN_FRONTEND="noninteractive"

apt-get update
apt-get upgrade -y
apt-get install -y ca-certificates curl zip unzip sudo gnupg tzdata

# Add docker APT repo
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install -y docker-ce-cli awscli

export DOCKER_BUILDKIT=1

echo "--- Build forge"

docker version

DEST_IMAGE="localhost:5000/rayci/forge"

tar --mtime="UTC 2020-01-01" -c -f - \
    ci/v2/forge/Dockerfile \
    | docker build --progress=plain -t "${DEST_IMAGE}" \
        -f ci/v2/forge/Dockerfile -

docker save -o forge.tgz "${DEST_IMAGE}"
echo "TEMP ECR: ${RAYCI_TEMP_ECR}"
echo "DEST IMAGE: ${DEST_IMAGE}"
docker push "${DEST_IMAGE}"

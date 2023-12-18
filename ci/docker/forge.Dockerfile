# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:20.04

ARG BUILDKITE_BAZEL_CACHE_URL

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/home/forge/.local/bin:${PATH}"
ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update
apt-get upgrade -y
apt-get install -y ca-certificates curl zip unzip sudo gnupg tzdata git

# Add docker client APT repository
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Add NodeJS APT repository
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -

# Install packages

apt-get update
apt-get install -y \
  awscli docker-ce-cli nodejs build-essential python-is-python3 \
  python3-pip openjdk-8-jre wget jq

# As a convention, we pin all python packages to a specific version. This
# is to to make sure we can control version upgrades through code changes.
python -m pip install pip==23.2.1

# Needs to be synchronized to the host group id as we map /var/run/docker.sock
# into the container.
addgroup --gid 1001 docker0  # Used on old buildkite AMIs.
addgroup --gid 993 docker

# Install bazelisk
npm install -g @bazel/bazelisk
ln -s /usr/local/bin/bazel /usr/local/bin/bazelisk

# A non-root user. Use 2000, which is the same as our buildkite agent VM uses.
adduser --home /home/forge --uid 2000 forge --gid 100
usermod -a -G docker0 forge
usermod -a -G docker forge

if [[ "$(uname -i)" == "x86_64" ]]; then
  curl -sfL "https://github.com/kubernetes-sigs/kind/releases/download/v0.11.1/kind-linux-amd64" -o /usr/local/bin/kind
  chmod +x /usr/local/bin/kind

  curl -sfL "https://dl.k8s.io/release/v1.28.4/bin/linux/amd64/kubectl" -o /usr/local/bin/kubectl
  chmod +x /usr/local/bin/kubectl

  curl -sfL "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv5.2.1/kustomize_v5.2.1_linux_amd64.tar.gz" \
      | tar -xzf - -C /usr/local/bin kustomize

  mkdir -p /usr/local/helm
  curl -sfL "https://get.helm.sh/helm-v3.12.2-linux-amd64.tar.gz" | tar -xzf - -C /usr/local/helm linux-amd64/helm
  ln -s /usr/local/helm/linux-amd64/helm /usr/local/bin/helm
fi

EOF

USER forge

RUN <<EOF
#!/bin/bash

set -euo pipefail

{
  echo "build --config=ci"
  echo "build --announce_rc"
  echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}"
} > ~/.bazelrc

EOF

CMD ["echo", "ray forge"]

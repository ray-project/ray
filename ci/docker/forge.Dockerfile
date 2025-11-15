# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:22.04

ARG BUILDKITE_BAZEL_CACHE_URL

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/home/forge/.local/bin:${PATH}"
ENV BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL}
ENV RAY_BUILD_ENV=ubuntu22.04_forge

RUN \
  --mount=type=bind,source=ci/k8s/install-k8s-tools.sh,target=install-k8s-tools.sh \
<<EOF
#!/bin/bash

set -euo pipefail

apt-get update
apt-get upgrade -y
apt-get install -y ca-certificates curl zip unzip sudo gnupg tzdata git apt-transport-https lsb-release

# Add docker client APT repository
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
# Download and install Microsoft signing key
curl -sLS https://packages.microsoft.com/keys/microsoft.asc |
  gpg --dearmor | tee /etc/apt/keyrings/microsoft.gpg > /dev/null
chmod go+r /etc/apt/keyrings/microsoft.gpg

echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Add NodeJS APT repository
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -

AZ_VER=2.72.0
AZ_DIST="$(lsb_release -cs)"

# Add Azure CLI repository
echo "Types: deb
URIs: https://packages.microsoft.com/repos/azure-cli/
Suites: ${AZ_DIST}
Components: main
Architectures: $(dpkg --print-architecture)
Signed-by: /etc/apt/keyrings/microsoft.gpg" | sudo tee /etc/apt/sources.list.d/azure-cli.sources

# Install packages

apt-get update
apt-get install -y \
  awscli nodejs build-essential python-is-python3 \
  python3-pip openjdk-8-jre wget jq \
  "docker-ce-cli=5:28.5.2-1~ubuntu.22.04~jammy" \
  azure-cli="${AZ_VER}"-1~"${AZ_DIST}"

# Install uv
wget -qO- https://astral.sh/uv/install.sh | sudo env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

mkdir -p /usr/local/python
# Install Python 3.10 using uv
uv python install --install-dir /usr/local/python 3.10
uv python pin 3.10

export UV_PYTHON_INSTALL_DIR=/usr/local/python
# Make Python 3.9 from uv the default by creating symlinks
UV_PYTHON_BIN="$(uv python find 3.10)"
ln -s "$UV_PYTHON_BIN" /usr/local/bin/python3.10
ln -s "$UV_PYTHON_BIN" /usr/local/bin/python3
ln -s "$UV_PYTHON_BIN" /usr/local/bin/python

# As a convention, we pin all python packages to a specific version. This
# is to to make sure we can control version upgrades through code changes.
uv pip install --system pip==25.0 cffi==1.16.0

# Needs to be synchronized to the host group id as we map /var/run/docker.sock
# into the container.
addgroup --gid 1001 docker0  # Used on old buildkite AMIs before 2023
addgroup --gid 993 docker1
addgroup --gid 992 docker    # buildkite AMI as of 2025-06-07

# Install bazelisk
npm install -g @bazel/bazelisk
ln -s /usr/local/bin/bazel /usr/local/bin/bazelisk

# A non-root user. Use 2000, which is the same as our buildkite agent VM uses.
adduser --home /home/forge --uid 2000 forge --gid 100
usermod -a -G docker0 forge
usermod -a -G docker1 forge
usermod -a -G docker forge

if [[ "$(uname -i)" == "x86_64" ]]; then
  bash install-k8s-tools.sh
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


# last update: 2025-11-12

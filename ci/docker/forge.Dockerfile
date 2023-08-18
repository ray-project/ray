# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:20.04

ARG BUILDKITE_BAZEL_CACHE_URL
ARG PYTHON_VERSION=3.8

ENV DEBIAN_FRONTEND=noninteractive
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
apt-get install -y awscli docker-ce-cli nodejs build-essential \
  openjdk-8-jre openjdk-8-jdk maven wget jq

# Install miniconda
wget --quiet \
    "https://repo.anaconda.com/miniconda/Miniconda3-py310_23.3.1-0-Linux-x86_64.sh" \
    -O /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -u -p /usr/local/conda
export PATH="/usr/local/conda/bin:${PATH}"
conda init
conda install -y python=$PYTHON_VERSION
conda clean -y --all

# As a convention, we pin all python packages to a specific version. This
# is to to make sure we can control version upgrades through code changes.
pip install pip==23.2.1

# Needs to be synchronized to the host group id as we map /var/run/docker.sock
# into the container.
addgroup --gid 1001 docker

# Install bazelisk
npm install -g @bazel/bazelisk
ln -s /usr/local/bin/bazel /usr/local/bin/bazelisk

# A non-root user. Use 2000, which is the same as our buildkite agent VM uses.
adduser --home /home/forge --uid 2000 forge --gid 100
usermod -a -G docker forge

EOF

USER forge

ENV PATH="/usr/local/conda/bin:/home/forge/.local/bin:${PATH}"

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

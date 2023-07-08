FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update
apt-get upgrade -y
apt-get install -y ca-certificates curl zip unzip sudo gnupg tzdata

# Add docker client APT repository
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list >/dev/null

# Add NodeJS APT repository
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -

# Install packages

apt-get update
apt-get install -y awscli docker-ce-cli nodejs
    
# Needs to be synchronized to the host group id as we map /var/run/docker.sock
# into the container.
addgroup --gid 1001 docker

# Install bazelisk
npm install -g @bazel/bazlisk
ln -s /usr/local/bin/bazel /usr/local/bin/bazelisk

EOF

RUN adduser --home /home/ray --uid 1000 ray --gid 100
RUN usermode -a -G docker ray

CMD ["echo", "ray forge"]
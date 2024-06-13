# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update
apt-get upgrade -y
apt-get install -y curl zip clang-12 git

# Needs to be synchronized to the host group id as we map /var/run/docker.sock
# into the container.
addgroup --gid 993 docker

ln -s /usr/bin/clang-12 /usr/bin/clang

# Install miniconda
curl -sfL https://repo.anaconda.com/miniconda/Miniconda3-py38_23.1.0-1-Linux-x86_64.sh > /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -u -p /usr/local/bin/miniconda3
rm /tmp/miniconda.sh
/usr/local/bin/miniconda3/bin/conda init bash

# Install Bazelisk
curl -L https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-amd64 --output /usr/local/bin/bazelisk
chmod +x /usr/local/bin/bazelisk

ln -s /usr/local/bin/bazelisk /usr/local/bin/bazel

# A non-root user. Use 2000, which is the same as our buildkite agent VM uses.
adduser --home /home/forge --uid 2000 forge --gid 100
usermod -a -G docker forge

EOF

USER forge
ENV CC=clang
ENV CXX=clang++-12

CMD ["echo", "ray release-automation forge"]

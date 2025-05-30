# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update
apt-get upgrade -y
apt-get install -y curl zip clang-12

ln -s /usr/bin/clang-12 /usr/bin/clang

# Install miniforge3
curl -sfL https://github.com/conda-forge/miniforge/releases/download/25.3.0-1/Miniforge3-25.3.0-1-Linux-aarch64.sh > /tmp/miniforge3.sh
bash /tmp/miniforge3.sh -b -u -p /usr/local/bin/miniforge3
rm /tmp/miniforge3.sh
/usr/local/bin/miniforge3/bin/conda init bash

# Install Bazelisk
curl -L https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-arm64 --output /usr/local/bin/bazelisk
chmod +x /usr/local/bin/bazelisk

ln -s /usr/local/bin/bazelisk /usr/local/bin/bazel

EOF

ENV CC=clang
ENV CXX=clang++-12

CMD ["echo", "ray release-automation forge"]

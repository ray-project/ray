# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update
apt-get upgrade -y
apt-get install -y -qq \
    curl zip clang-format-12 \
    clang-tidy-12 clang-12

ln -s /usr/bin/clang-format-12 /usr/bin/clang-format && \
ln -s /usr/bin/clang-tidy-12 /usr/bin/clang-tidy && \
ln -s /usr/bin/clang-12 /usr/bin/clang

# Install miniconda
curl -sfL https://repo.anaconda.com/miniconda/Miniconda3-py38_23.1.0-1-Linux-aarch64.sh > /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -u -p /root/miniconda3
rm /tmp/miniconda.sh
/root/miniconda3/bin/conda init bash

# Install Bazelisk
curl -L https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-arm64 --output bazelisk
chmod +x bazelisk

mv bazelisk /usr/bin/
ln -s /usr/bin/bazelisk /usr/bin/bazel

EOF

ENV CC=clang
ENV CXX=clang++-12
ENV USE_BAZEL_VERSION=5.4.1

CMD ["echo", "ray release-automation forge"]

# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update
apt-get upgrade -y
apt-get install -y curl zip clang-12

ln -s /usr/bin/clang-12 /usr/bin/clang

# Install miniforge3
curl -fsSL https://github.com/conda-forge/miniforge/releases/download/25.3.0-1/Miniforge3-25.3.0-1-Linux-aarch64.sh > /tmp/miniforge3.sh
bash /tmp/miniforge3.sh -b -u -p /usr/local/bin/miniforge3
rm /tmp/miniforge3.sh
/usr/local/bin/miniforge3/bin/conda init bash

# Install Bazelisk
curl -fsSL https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-arm64 --output /usr/local/bin/bazelisk
chmod +x /usr/local/bin/bazelisk

ln -s /usr/local/bin/bazelisk /usr/local/bin/bazel

# Install uv
curl -fsSL https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

mkdir -p /usr/local/python
# Install Python using uv
UV_PYTHON_VERSION=3.10
uv python install --install-dir /usr/local/python "$UV_PYTHON_VERSION"

export UV_PYTHON_INSTALL_DIR=/usr/local/python
# Make Python from uv the default by creating symlinks
UV_PYTHON_BIN="$(uv python find --no-project "$UV_PYTHON_VERSION")"
echo "uv python binary location: $UV_PYTHON_BIN"
ln -s "$UV_PYTHON_BIN" "/usr/local/bin/python${UV_PYTHON_VERSION}"
ln -s "$UV_PYTHON_BIN" /usr/local/bin/python3
ln -s "$UV_PYTHON_BIN" /usr/local/bin/python

# As a convention, we pin all python packages to a specific version. This
# is to to make sure we can control version upgrades through code changes.
uv pip install --system pip==25.2 cffi==1.16.0

EOF

ENV CC=clang
ENV CXX=clang++-12

CMD ["echo", "ray release-automation forge"]

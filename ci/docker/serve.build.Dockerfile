# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ENABLE_TRACING
ARG PYDANTIC_VERSION
ARG PYTHON

SHELL ["/bin/bash", "-ice"]

COPY . .

# Install HAProxy from source
RUN <<EOF
#!/bin/bash
set -euo pipefail

# Install HAProxy dependencies
sudo apt-get update && sudo apt-get install -y \
    build-essential \
    curl \
    libc6-dev \
    liblua5.3-dev \
    libpcre3-dev \
    libssl-dev \
    socat \
    wget \
    zlib1g-dev \
    && sudo rm -rf /var/lib/apt/lists/*

# Create haproxy user and group
sudo groupadd -r haproxy
sudo useradd -r -g haproxy haproxy

# Download and compile HAProxy from official source
HAPROXY_VERSION="2.8.12"
HAPROXY_BUILD_DIR="$(mktemp -d)"
wget -O "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" "https://www.haproxy.org/download/2.8/src/haproxy-${HAPROXY_VERSION}.tar.gz"
tar -xzf "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" -C "${HAPROXY_BUILD_DIR}" --strip-components=1
make -C "${HAPROXY_BUILD_DIR}" TARGET=linux-glibc USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1
sudo make -C "${HAPROXY_BUILD_DIR}" install
rm -rf "${HAPROXY_BUILD_DIR}"

# Create HAProxy directories
sudo mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy
sudo chown -R haproxy:haproxy /run/haproxy

# Allow the ray user to manage HAProxy files without password
echo "ray ALL=(ALL) NOPASSWD: /bin/cp * /etc/haproxy/*, /bin/touch /etc/haproxy/*, /usr/local/sbin/haproxy*" | sudo tee /etc/sudoers.d/haproxy-ray

EOF

RUN <<EOF
#!/bin/bash

set -euo pipefail

pip install -U --ignore-installed \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt

# TODO(can): upgrade tensorflow for python 3.12
if [[ "${PYTHON-}" != "3.12" ]]; then
  pip install -U -c python/requirements_compiled.txt \
    tensorflow tensorflow-probability torch torchvision \
    transformers aioboto3
fi

git clone --branch=4.2.0 --depth=1 https://github.com/wg/wrk.git /tmp/wrk
make -C /tmp/wrk -j
sudo cp /tmp/wrk/wrk /usr/local/bin/wrk
rm -rf /tmp/wrk

# Install custom Pydantic version if requested.
if [[ -n "${PYDANTIC_VERSION-}" ]]; then
  pip install -U pydantic==$PYDANTIC_VERSION
else
  echo "Not installing Pydantic from source"
fi

if [[ "${ENABLE_TRACING-}" == "1" ]]; then
  # Install tracing dependencies if requested. Intentionally, we do not use
  # requirements_compiled.txt as the constraint file. They are not compatible with
  # a few packages in that file (e.g. requiring an ugprade to protobuf 5+).
  pip install opentelemetry-exporter-otlp==1.34.1
else
  echo "Not installing tracing dependencies"
fi

EOF

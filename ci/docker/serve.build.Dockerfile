# syntax=docker/dockerfile:1.3-labs

# Build HAProxy in a separate stage so build deps don't bloat the final image
ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD AS haproxy-builder

RUN <<EOF
#!/bin/bash
set -euo pipefail

sudo apt-get update -y
sudo apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    libc6-dev \
    liblua5.3-dev \
    libpcre3-dev \
    libssl-dev \
    wget \
    zlib1g-dev

sudo rm -rf /var/lib/apt/lists/*

HAPROXY_VERSION="2.8.12"
HAPROXY_BUILD_DIR=$(mktemp -d)
wget -O "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" "https://www.haproxy.org/download/2.8/src/haproxy-${HAPROXY_VERSION}.tar.gz"
tar -xzf "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" -C "${HAPROXY_BUILD_DIR}" --strip-components=1
make -C "${HAPROXY_BUILD_DIR}" TARGET=linux-glibc USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc)
sudo make -C "${HAPROXY_BUILD_DIR}" install
rm -rf "${HAPROXY_BUILD_DIR}"
EOF

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ENABLE_TRACING
ARG PYDANTIC_VERSION
ARG PYTHON

SHELL ["/bin/bash", "-ice"]

COPY . .

# Copy HAProxy binary from builder stage
COPY --from=haproxy-builder /usr/local/sbin/haproxy /usr/local/sbin/haproxy

# Install HAProxy runtime deps and setup
RUN <<EOF
#!/bin/bash
set -euo pipefail

sudo apt-get update && sudo apt-get install -y --no-install-recommends socat liblua5.3-0
sudo rm -rf /var/lib/apt/lists/*

sudo groupadd -r haproxy
sudo useradd -r -g haproxy haproxy

sudo mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy
sudo chown -R haproxy:haproxy /run/haproxy

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

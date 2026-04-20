# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD AS haproxy-builder

RUN <<EOF
#!/bin/bash
set -euo pipefail

apt-get update -y
apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    libc6-dev \
    liblua5.3-dev \
    libpcre3-dev \
    libssl-dev \
    zlib1g-dev

# Install HAProxy from source.
# Fetched from git.haproxy.org because (a) www.haproxy.org has not published the
# 2.8.20 release tarball and (b) the *.haproxy.org TLS cert expired 2026-04-17.
# Integrity is enforced by sha256 verification, so -k (skip TLS verify) is safe
# here. Drop -k once the cert is renewed; keep the sha256 pin.
HAPROXY_VERSION="2.8.20"
HAPROXY_SHA256="c8301de11dabfbf049db07080e43b9570a63f99e41d4b0754760656bf7ea00b7"
HAPROXY_BUILD_DIR=$(mktemp -d)
curl --retry 5 --retry-all-errors --connect-timeout 20 --max-time 300 \
     -k -sSfL -o "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" \
     "https://git.haproxy.org/?p=haproxy-2.8.git;a=snapshot;h=refs/tags/v${HAPROXY_VERSION};sf=tgz"
echo "${HAPROXY_SHA256}  ${HAPROXY_BUILD_DIR}/haproxy.tar.gz" | sha256sum -c -
tar -xzf "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" -C "${HAPROXY_BUILD_DIR}" --strip-components=1
make -C "${HAPROXY_BUILD_DIR}" TARGET=linux-glibc USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc)
make -C "${HAPROXY_BUILD_DIR}" install
rm -rf "${HAPROXY_BUILD_DIR}"
EOF

FROM $DOCKER_IMAGE_BASE_BUILD

ARG ENABLE_TRACING
ARG PYDANTIC_VERSION
ARG IMAGE_TYPE="base"
ARG PYTHON
ARG PYTHON_DEPSET="python/deplocks/ci/serve_${IMAGE_TYPE}_depset_py${PYTHON}.lock"

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY --from=haproxy-builder /usr/local/sbin/haproxy /usr/local/sbin/haproxy

RUN <<EOF
#!/bin/bash
set -euo pipefail

apt-get update -y && apt-get install -y --no-install-recommends liblua5.3-0 libpcre3
rm -rf /var/lib/apt/lists/*
mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy
EOF

RUN <<EOF
#!/bin/bash

set -euo pipefail

uv pip install --system --no-cache-dir --no-deps --index-strategy unsafe-best-match \
    -r /home/ray/python_depset.lock

git clone --branch=4.2.0 --depth=1 https://github.com/wg/wrk.git /tmp/wrk
make -C /tmp/wrk -j
sudo cp /tmp/wrk/wrk /usr/local/bin/wrk
rm -rf /tmp/wrk

EOF

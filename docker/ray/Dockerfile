# syntax=docker/dockerfile:1.3-labs

ARG FULL_BASE_IMAGE

# --- HAProxy Build Stage ---
FROM $FULL_BASE_IMAGE AS haproxy-builder

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
rm -rf /var/lib/apt/lists/*

# Install HAProxy from source
HAPROXY_VERSION="2.8.12"
BUILD_DIR=$(mktemp -d)
curl -sSfL -o "${BUILD_DIR}/haproxy.tar.gz" "https://www.haproxy.org/download/2.8/src/haproxy-${HAPROXY_VERSION}.tar.gz"
tar -xzf "${BUILD_DIR}/haproxy.tar.gz" -C "${BUILD_DIR}" --strip-components=1
make -C "${BUILD_DIR}" TARGET=linux-glibc USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc)
make -C "${BUILD_DIR}" install SBINDIR=/usr/local/bin
rm -rf "${BUILD_DIR}"

EOF

# --- Return to main image ---
FROM $FULL_BASE_IMAGE

# Switch to root for system-level HAProxy setup
USER root

# Copy HAProxy binary from builder stage
COPY --from=haproxy-builder /usr/local/bin/haproxy /usr/local/bin/haproxy

# Install HAProxy runtime dependency and setup
RUN <<EOF
#!/bin/bash
set -euo pipefail

apt-get update -y
apt-get install -y --no-install-recommends socat liblua5.3-0

mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy
chown -R ray:"$(id -gn ray)" /run/haproxy

rm -rf /var/lib/apt/lists/*

EOF

USER ray

ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
ARG CONSTRAINTS_FILE="requirements_compiled.txt"

COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -c $CONSTRAINTS_FILE \
    `basename $WHEEL_PATH`[all] \
    --find-links $FIND_LINKS_PATH && sudo rm `basename $WHEEL_PATH`

RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

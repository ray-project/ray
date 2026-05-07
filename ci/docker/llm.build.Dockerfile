# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.11
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

HAPROXY_VERSION="2.8.20"
HAPROXY_SHA256="c8301de11dabfbf049db07080e43b9570a63f99e41d4b0754760656bf7ea00b7"
HAPROXY_BUILD_DIR=$(mktemp -d)
curl --retry 5 --retry-all-errors --connect-timeout 20 --max-time 300 \
     -sSfL -o "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" \
     "https://github.com/ray-project/haproxy-release/releases/download/${HAPROXY_VERSION}/haproxy-${HAPROXY_VERSION}.tar.gz"
echo "${HAPROXY_SHA256}  ${HAPROXY_BUILD_DIR}/haproxy.tar.gz" | sha256sum -c -
tar -xzf "${HAPROXY_BUILD_DIR}/haproxy.tar.gz" -C "${HAPROXY_BUILD_DIR}" --strip-components=1
make -C "${HAPROXY_BUILD_DIR}" TARGET=linux-glibc USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc)
make -C "${HAPROXY_BUILD_DIR}" install
rm -rf "${HAPROXY_BUILD_DIR}"
EOF

FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAY_CI_JAVA_BUILD=
ARG RAY_CUDA_CODE=cpu

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY --from=haproxy-builder /usr/local/sbin/haproxy /usr/local/sbin/haproxy

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -y && apt-get install -y --no-install-recommends liblua5.3-0 libpcre3
rm -rf /var/lib/apt/lists/*
mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy

SKIP_PYTHON_PACKAGES=1 ./ci/env/install-dependencies.sh

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
pip install --no-deps -r python/deplocks/llm/rayllm_test_${PYTHON_CODE}_${RAY_CUDA_CODE}.lock

EOF

# Use the revamped ray executor backend in vLLM
ENV VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1

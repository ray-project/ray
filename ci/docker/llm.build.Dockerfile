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

# Install HAProxy from source.
# Fetched from ray-project/haproxy-release (a GitHub release mirror) because
# www.haproxy.org's wildcard TLS cert expired 2026-04-17 and the release tarball
# disappeared from the upstream download site. Integrity is enforced by sha256
# verification. Drop this mirror and switch back to www.haproxy.org once the
# cert is renewed and the tarball is republished.
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
EOF

RUN <<EOF
#!/bin/bash

set -euo pipefail

SKIP_PYTHON_PACKAGES=1 ./ci/env/install-dependencies.sh

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
pip install --no-deps -r python/deplocks/llm/rayllm_test_${PYTHON_CODE}_${RAY_CUDA_CODE}.lock

# Overlay the Python files changed by vLLM PR #45026 ("Stop setting
# CUDA_VISIBLE_DEVICES internally in vLLM, add device_ids arg") on top of the
# installed vllm 0.23.0 wheel, so the Ray LLM release tests validate the CVD
# rework. The cvd-fix branch is the PR's net diff cherry-picked onto
# releases/v0.23.0 (== the v0.23.0 tag the wheel is built from), so every file
# below is "0.23.0 + the PR's changes" and overlays the wheel cleanly.
# This supersedes the old vllm-cuda-visible-devices-patch (vLLM PR #44466),
# which took the opposite approach (kept setting CVD per worker); the two
# cannot coexist. The PR's sm100_cutlass_mla_kernel.cu change is omitted: it
# requires recompiling the wheel and only affects SM100/MLA.
VLLM_SITE="$(python -c 'import vllm, os; print(os.path.dirname(vllm.__file__))')"
git clone --depth 1 -b cvd-fix https://github.com/jeffreywang88/vllm.git /tmp/vllm-cvd-overlay
for f in \
    config/parallel.py \
    distributed/device_communicators/all2all.py \
    distributed/device_communicators/all_reduce_utils.py \
    distributed/device_communicators/custom_all_reduce.py \
    distributed/device_communicators/quick_all_reduce.py \
    distributed/device_communicators/shm_broadcast.py \
    distributed/kv_transfer/kv_connector/v1/lmcache_integration/vllm_v1_adapter.py \
    distributed/parallel_state.py \
    distributed/stateless_coordinator.py \
    engine/arg_utils.py \
    entrypoints/openai/dp_supervisor.py \
    platforms/cuda.py \
    platforms/interface.py \
    v1/engine/core.py \
    v1/engine/utils.py \
    v1/executor/multiproc_executor.py \
    v1/executor/ray_executor.py \
    v1/executor/ray_executor_v2.py \
    v1/executor/ray_utils.py \
    v1/worker/gpu_worker.py \
    v1/worker/worker_base.py; do
    cp "/tmp/vllm-cvd-overlay/vllm/${f}" "${VLLM_SITE}/${f}"
done
rm -rf /tmp/vllm-cvd-overlay

EOF


# vLLM 0.21.0 selects the FlashInfer top-k/top-p sampler during engine initialization
# instead of the previous PyTorch-native/Triton sampling path. The FlashInfer sampler
# introduces longer adds a large one-time engine initialization cost. To avoid performance
# surprises, we disable the FlashInfer sampler by default.
ENV VLLM_USE_FLASHINFER_SAMPLER=0

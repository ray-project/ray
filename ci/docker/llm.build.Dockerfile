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

if [[ "${RAY_CUDA_CODE}" == "cu130" ]]; then
    # Keep the NIXL CUDA 13 backend and NIXL EP package available, while
    # removing stale CUDA 12 NIXL files that can be left behind by overlapping
    # nixl-cu12/nixl-cu13 package layouts.
    pip uninstall -y nixl-cu12 nixl-cu13 || true
    pip install --no-cache-dir --no-deps --force-reinstall nixl-cu13==1.1.0

    python - <<'PY'
import importlib.metadata
import importlib.util
import shutil
import site
from pathlib import Path

for site_dir in map(Path, site.getsitepackages()):
    for pattern in (
        "nixl_cu12",
        "nixl_cu12-*",
        "nixl_cu12.libs",
        ".nixl_cu12.*",
    ):
        for path in site_dir.glob(pattern):
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()

importlib.invalidate_caches()

installed = {
    dist.metadata.get("Name", "").lower()
    for dist in importlib.metadata.distributions()
}
if "nixl-cu12" in installed:
    raise SystemExit("nixl-cu12 must not be installed in the CUDA 13 LLM image")
if "nixl" not in installed:
    raise SystemExit("nixl must be installed for vLLM KV connector tests")
if "nixl-cu13" not in installed:
    raise SystemExit("nixl-cu13 must be installed in the CUDA 13 LLM image")

if importlib.util.find_spec("nixl") is None:
    raise SystemExit("nixl import spec is missing")
if importlib.util.find_spec("nixl_cu13") is None:
    raise SystemExit("nixl_cu13 import spec is missing")
if importlib.util.find_spec("nixl_ep") is None:
    raise SystemExit("nixl_ep import spec is missing")

nixl_paths = []
for site_dir in map(Path, site.getsitepackages()):
    for pattern in ("nixl_ep", "nixl_cu13", "nixl_cu13.libs", ".nixl_cu13.*"):
        nixl_paths.extend(site_dir.glob(pattern))

bad_paths = []
good_paths = []
for path in nixl_paths:
    files = path.rglob("*") if path.is_dir() else [path]
    for file in files:
        if not file.is_file():
            continue
        payload = file.read_bytes()
        if b"libcudart.so.12" in payload:
            bad_paths.append(str(file))
        if b"libcudart.so.13" in payload:
            good_paths.append(str(file))

if bad_paths:
    raise SystemExit(f"CUDA 12 NIXL files found in CUDA 13 LLM image: {bad_paths}")
if not good_paths:
    raise SystemExit("No CUDA 13 NIXL binaries found in the CUDA 13 LLM image")

print("Verified CUDA 13 NIXL backend with nixl_ep enabled")
PY
fi

EOF

ENV VLLM_USE_FLASHINFER_SAMPLER=0

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

# Backport vLLM PR #39873 until the pinned vLLM release includes it.
python - <<'PY'
import site
import sysconfig
from pathlib import Path

ref = "9a77e42a670baf89871ff1d49aa4247b7749024f"
candidate_dirs = [
    Path(sysconfig.get_paths()["purelib"]),
    Path(sysconfig.get_paths()["platlib"]),
    *(Path(path) for path in site.getsitepackages()),
]

for base_dir in dict.fromkeys(candidate_dirs):
    import_utils = base_dir / "vllm" / "utils" / "import_utils.py"
    if import_utils.exists():
        break
else:
    raise SystemExit("vLLM import_utils.py not found")

old = '''@cache
def _has_module(module_name: str) -> bool:
    """Return True if *module_name* can be found in the current environment.

    The result is cached so that subsequent queries for the same module incur
    no additional overhead.
    """
    return importlib.util.find_spec(module_name) is not None
'''
new = '''@cache
def _has_module(module_name: str) -> bool:
    """Return True if *module_name* can be imported in the current environment.

    Uses ``importlib.util.find_spec`` as a fast pre-check, then performs a
    trial import to verify that native dependencies (shared libraries, etc.)
    are also satisfied.  The result is cached so that subsequent queries for
    the same module incur no additional overhead.
    """
    if importlib.util.find_spec(module_name) is None:
        return False
    try:
        importlib.import_module(module_name)
        return True
    except (ImportError, OSError) as exc:
        logger.debug("Module %s was found but failed to import: %s", module_name, exc)
        return False
'''
text = import_utils.read_text()
if old in text:
    import_utils.write_text(text.replace(old, new))
elif "importlib.import_module(module_name)" not in text:
    raise SystemExit("Could not apply vLLM optional import fix")

print(f"Backported vLLM optional import fix {ref} to {import_utils}")
PY

EOF

ENV VLLM_USE_FLASHINFER_SAMPLER=0

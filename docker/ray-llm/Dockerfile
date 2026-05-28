# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/deplocks/llm/rayllm_*.lock ./

# vLLM version tag to use for EP kernel and DeepGEMM install scripts
# Keep in sync with vllm version in python/requirements/llm/llm-requirements.txt
ARG VLLM_SCRIPTS_REF="v0.21.0"

RUN <<EOF
#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"

if [[ "${PYTHON_CODE}" == "py312" ]]; then
    CUDA_CODE=cu130
    # Use nvshmem 3.3.24 which is the default for vLLM and compatible with CUDA 13
    # https://github.com/vllm-project/vllm/blob/64ac1395e8d52e3e38910a62c7eb8524126730d8/tools/ep_kernels/install_python_libraries.sh#L14
    NVSHMEM_VER=3.3.24
else
    echo "ray-llm supports Python 3.12 only (this image is ${PYTHON_CODE})."
    exit 1
fi

# Hash verification is disabled because uv pip compile generates hashes from
# PyPI, but unsafe-best-match may download from the CUDA index which serves
# different builds of some packages (e.g. triton). The lock file still pins
# exact versions, so integrity is maintained through version pinning.
uv pip install --system --no-cache-dir --no-deps \
    --index-strategy unsafe-best-match \
    --no-verify-hashes \
    -r "rayllm_${PYTHON_CODE}_${CUDA_CODE}.lock"

sudo apt-get update -y && sudo apt-get install -y curl kmod pkg-config librdmacm-dev cmake

# Fetch and run vLLM install scripts at pinned commit
VLLM_RAW="https://raw.githubusercontent.com/vllm-project/vllm/${VLLM_SCRIPTS_REF}"

# Tell uv to use system Python since the vLLM scripts use uv
export UV_SYSTEM_PYTHON=1

# Set CUDA architectures for building EP kernels
# EP kernels + DeepGEMM require Hopper+ features (matches vLLM Dockerfile)
export TORCH_CUDA_ARCH_LIST="9.0a 10.0a"

# Install EP kernels (PPLX, DeepEP, and NVSHMEM)
curl -fsSL "${VLLM_RAW}/tools/ep_kernels/install_python_libraries.sh" | \
    bash -s -- --workspace /home/ray/llm_ep_support --nvshmem-ver ${NVSHMEM_VER}

# Install DeepGEMM
curl -fsSL "${VLLM_RAW}/tools/install_deepgemm.sh" | bash

# Keep the release image capable of running NIXL EP kernels.
uv pip uninstall --system nixl-cu12 nixl-cu13 || true
uv pip install --system --no-cache-dir --no-deps --reinstall nixl-cu13==1.1.0

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

# Export installed packages
$HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean

EOF

ENV VLLM_USE_FLASHINFER_SAMPLER=0

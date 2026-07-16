# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/deplocks/llm/rayllm_*.lock ./
COPY python/requirements/llm/patches/vllm-device-aware-compile-cache.patch ./

# vLLM version tag to use for EP kernel and DeepGEMM install scripts
# Keep in sync with vllm version in python/requirements/llm/llm-requirements.txt
ARG VLLM_SCRIPTS_REF="v0.25.1"

# Pin DeepEP to the V1 (NVSHMEM) commit that vLLM's own release image uses. The
# install script's default drifted to DeepEP V2 ("NCCL Gin"), which needs
# NCCL >= 2.30.4, but our image has nvidia-nccl-cu13==2.28.9 (via torch) so V2
# fails to build. Keep in sync with DEEPEP_COMMIT_HASH in vllm's docker/Dockerfile.
ARG DEEPEP_COMMIT_HASH="73b6ea4"

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

# Include the CUDA device index in vLLM's compile cache paths so a worker never
# reloads a torch.compile artifact built for a different physical GPU.
# TODO (jeffreywang): Remove this patch once https://github.com/vllm-project/vllm/pull/38962 lands.
VLLM_DEVICE_AWARE_COMPILE_CACHE_PATCH="$(pwd)/vllm-device-aware-compile-cache.patch"
VLLM_SITE_PACKAGES="$(python - <<'PY'
import site
import sysconfig
from pathlib import Path

candidate_dirs = [
    Path(sysconfig.get_paths()["purelib"]),
    Path(sysconfig.get_paths()["platlib"]),
    *(Path(path) for path in site.getsitepackages()),
]

for base_dir in dict.fromkeys(candidate_dirs):
    import_utils = base_dir / "vllm" / "utils" / "import_utils.py"
    if import_utils.exists():
        print(base_dir)
        break
else:
    raise SystemExit("vLLM import_utils.py not found")
PY
)"
(
    cd "${VLLM_SITE_PACKAGES}"
    git apply "${VLLM_DEVICE_AWARE_COMPILE_CACHE_PATCH}"
)

sudo apt-get update -y && sudo apt-get install -y curl kmod pkg-config librdmacm-dev cmake

# Fetch and run vLLM install scripts at pinned commit
VLLM_RAW="https://raw.githubusercontent.com/vllm-project/vllm/${VLLM_SCRIPTS_REF}"

# Tell uv to use system Python since the vLLM scripts use uv
export UV_SYSTEM_PYTHON=1

# Set CUDA architectures for building EP kernels
# EP kernels + DeepGEMM require Hopper+ features (matches vLLM Dockerfile)
export TORCH_CUDA_ARCH_LIST="9.0a 10.0a"

# Install EP kernels (PPLX, DeepEP, and NVSHMEM)
# Pin --deepep-ref to the V1 NVSHMEM-backend commit (see DEEPEP_COMMIT_HASH above);
# the script's own default is the V2 NCCL-Gin backend that fails to compile here.
curl -fsSL "${VLLM_RAW}/tools/ep_kernels/install_python_libraries.sh" | \
    bash -s -- --workspace /home/ray/llm_ep_support --nvshmem-ver ${NVSHMEM_VER} --deepep-ref ${DEEPEP_COMMIT_HASH}

# Install DeepGEMM
curl -fsSL "${VLLM_RAW}/tools/install_deepgemm.sh" | bash

# Export installed packages
$HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

sudo rm -rf /var/lib/apt/lists/*
sudo apt-get clean

EOF


# vLLM 0.21.0 selects the FlashInfer top-k/top-p sampler during engine initialization
# instead of the previous PyTorch-native/Triton sampling path. The FlashInfer sampler
# introduces longer adds a large one-time engine initialization cost. To avoid performance
# surprises, we disable the FlashInfer sampler by default.
ENV VLLM_USE_FLASHINFER_SAMPLER=0

# vLLM 0.25.1 defaults to Model Runner V2, which skips multimodal encoder memory
# profiling and OOMs the LLM GPU tests. Force Model Runner V1 for now.
# TODO (jeffreywang): Use the default MRV2 once https://github.com/vllm-project/vllm/pull/47985 lands.
ENV VLLM_USE_V2_MODEL_RUNNER=0

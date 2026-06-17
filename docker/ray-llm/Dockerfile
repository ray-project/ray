# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

COPY python/deplocks/llm/rayllm_*.lock ./

# vLLM version tag to use for EP kernel and DeepGEMM install scripts
# Keep in sync with vllm version in python/requirements/llm/llm-requirements.txt
ARG VLLM_SCRIPTS_REF="v0.23.0"

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

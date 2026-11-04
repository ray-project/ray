# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.11
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAY_CI_JAVA_BUILD=
ARG RAY_CUDA_CODE=cpu

# vLLM version tag to fetch install scripts from.
# Keep in sync with vllm version in python/requirements/llm/llm-requirements.txt
ARG VLLM_SCRIPTS_REF="v0.32.0"

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

SKIP_PYTHON_PACKAGES=1 ./ci/env/install-dependencies.sh

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
pip install --no-deps -r python/deplocks/llm/rayllm_test_${PYTHON_CODE}_${RAY_CUDA_CODE}.lock

if [[ "${RAY_CUDA_CODE}" != "cpu" ]]; then
    # Install FlashInfer's precompiled kernels (flashinfer-cubin and
    # flashinfer-jit-cache, matching the locked flashinfer-python) for all GPU
    # architectures, as vLLM's own image does. Without them, FlashInfer
    # JIT-compiles kernels on first use, which can stall engine startup for
    # many minutes.
    PIP_NO_CACHE_DIR=1 flashinfer download-kernels

    # libnuma-dev and numactl match vLLM's image: fastsafetensors loads libnuma,
    # and vLLM's NUMA binding (numa_bind) launches workers under the numactl CLI.
    apt-get update -y
    apt-get install -y libnuma-dev numactl
    rm -rf /var/lib/apt/lists/*

    # Install gdrcopy's user-space library (libgdrapi) as vLLM's image does.
    # NVSHMEM (DeepEP) and UCX (NIXL) use it for low-latency GPU memory copies on
    # hosts that load the gdrdrv kernel module. vLLM uses the CUDA 12.8 build on
    # CUDA 13 too.
    GDRCOPY_OS_VERSION="Ubuntu$(. /etc/os-release && echo "${VERSION_ID//./_}")"
    curl -fsSL "https://raw.githubusercontent.com/vllm-project/vllm/${VLLM_SCRIPTS_REF}/tools/install_gdrcopy.sh" | \
        bash -s -- "${GDRCOPY_OS_VERSION}" 12.8 "$(uname -m)"
fi

# Include the CUDA device index in vLLM's compile cache paths so a worker never
# reloads a torch.compile artifact built for a different physical GPU.
# vllm-project/vllm#38962 was reverted by #53304; still needed in vLLM 0.32.0.
VLLM_DEVICE_AWARE_COMPILE_CACHE_PATCH="$(pwd)/python/requirements/llm/patches/vllm-device-aware-compile-cache.patch"
VLLM_DEFER_TRITON_PATCH="$(pwd)/python/requirements/llm/patches/vllm-defer-triton-validation.patch"
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
    # Validate Triton on inference workers so CPU frontends can build configs.
    # TODO (jeffreywang): Remove this patch once vLLM PR #62576 is included in Ray's pinned version.
    # https://github.com/vllm-project/vllm/pull/62576
    git apply "${VLLM_DEFER_TRITON_PATCH}"
)

EOF


# vLLM 0.21.0 selects the FlashInfer top-k/top-p sampler during engine initialization
# instead of the previous PyTorch-native/Triton sampling path. The FlashInfer sampler
# introduces longer adds a large one-time engine initialization cost. To avoid performance
# surprises, we disable the FlashInfer sampler by default.
ENV VLLM_USE_FLASHINFER_SAMPLER=0

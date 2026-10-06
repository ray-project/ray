# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.11
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAY_CI_JAVA_BUILD=
ARG RAY_CUDA_CODE=cpu

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

SKIP_PYTHON_PACKAGES=1 ./ci/env/install-dependencies.sh

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
pip install --no-deps -r python/deplocks/llm/rayllm_test_${PYTHON_CODE}_${RAY_CUDA_CODE}.lock

# Include the CUDA device index in vLLM's compile cache paths so a worker never
# reloads a torch.compile artifact built for a different physical GPU.
# TODO (jeffreywang): Remove this patch once https://github.com/vllm-project/vllm/pull/38962 lands.
VLLM_DEVICE_AWARE_COMPILE_CACHE_PATCH="$(pwd)/python/requirements/llm/patches/vllm-device-aware-compile-cache.patch"
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

EOF


# vLLM 0.21.0 selects the FlashInfer top-k/top-p sampler during engine initialization
# instead of the previous PyTorch-native/Triton sampling path. The FlashInfer sampler
# introduces longer adds a large one-time engine initialization cost. To avoid performance
# surprises, we disable the FlashInfer sampler by default.
ENV VLLM_USE_FLASHINFER_SAMPLER=0

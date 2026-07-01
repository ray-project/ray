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

# NOTE: The RayExecutorV2 GPU-collision workaround (vllm-cuda-visible-devices-patch)
# was dropped for vLLM 0.24.0, which merged vllm-project/vllm#44466 (workers now
# receive an explicit logical-to-physical GPU mapping instead of vLLM mutating
# CUDA_VISIBLE_DEVICES). The patch is obsolete and no longer applies.

EOF


# vLLM 0.21.0 selects the FlashInfer top-k/top-p sampler during engine initialization
# instead of the previous PyTorch-native/Triton sampling path. The FlashInfer sampler
# introduces longer adds a large one-time engine initialization cost. To avoid performance
# surprises, we disable the FlashInfer sampler by default.
ENV VLLM_USE_FLASHINFER_SAMPLER=0

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

# Overlay only the files changed by the vLLM RayExecutorV2 PR
# (https://github.com/vllm-project/vllm/pull/36836) on top of the installed
# vllm 0.18.0 wheel. The ray-rebase-release-v0.18.0 branch contains only the
# RayExecutorV2 commits cherry-picked onto releases/v0.18.0, so every changed
# file is safe to copy without overwriting compiled C extensions.
VLLM_SITE="$(python -c 'import vllm, os; print(os.path.dirname(vllm.__file__))')"
git clone --depth 1 -b ray-rebase-release-v0.18.0 https://github.com/jeffreywang-anyscale/vllm.git /tmp/vllm-overlay
cp /tmp/vllm-overlay/vllm/envs.py "${VLLM_SITE}/envs.py"
cp /tmp/vllm-overlay/vllm/config/parallel.py "${VLLM_SITE}/config/parallel.py"
cp /tmp/vllm-overlay/vllm/config/vllm.py "${VLLM_SITE}/config/vllm.py"
cp /tmp/vllm-overlay/vllm/v1/engine/core.py "${VLLM_SITE}/v1/engine/core.py"
cp /tmp/vllm-overlay/vllm/v1/executor/abstract.py "${VLLM_SITE}/v1/executor/abstract.py"
cp /tmp/vllm-overlay/vllm/v1/executor/ray_executor_v2.py "${VLLM_SITE}/v1/executor/ray_executor_v2.py"
cp /tmp/vllm-overlay/vllm/v1/executor/ray_utils.py "${VLLM_SITE}/v1/executor/ray_utils.py"
cp /tmp/vllm-overlay/vllm/v1/worker/gpu_worker.py "${VLLM_SITE}/v1/worker/gpu_worker.py"
cp /tmp/vllm-overlay/vllm/v1/worker/worker_base.py "${VLLM_SITE}/v1/worker/worker_base.py"
cp /tmp/vllm-overlay/vllm/v1/executor/ray_env_utils.py "${VLLM_SITE}/v1/executor/ray_env_utils.py"
rm -rf /tmp/vllm-overlay

EOF

# Conda's libstdc++ provides CXXABI_1.3.15 needed by ICU 78 and other
# C++ libraries pulled in by vLLM 0.17.0. Place it before the system copy
# so the dynamic linker finds it first.
ENV LD_LIBRARY_PATH=/home/ray/anaconda3/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
ENV VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1

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

VLLM_SITE="$(python -c 'import vllm, os; print(os.path.dirname(vllm.__file__))')"
git clone --depth 1 -b lazy-import https://github.com/jeffreywang-anyscale/vllm.git /tmp/vllm-overlay
cp /tmp/vllm-overlay/vllm/v1/structured_output/utils.py "${VLLM_SITE}/v1/structured_output/utils.py"
rm -rf /tmp/vllm-overlay

EOF

# Conda's libstdc++ provides CXXABI_1.3.15 needed by ICU 78 and other
# C++ libraries pulled in by vLLM 0.17.0. Place it before the system copy
# so the dynamic linker finds it first.
# ENV LD_LIBRARY_PATH=/home/ray/anaconda3/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}

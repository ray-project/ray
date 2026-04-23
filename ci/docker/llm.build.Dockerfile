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

EOF

# vLLM 0.17.0 eagerly imports xgrammar -> ICU 78 -> CXXABI_1.3.15.
# System libstdc++ only has 1.3.13; we need conda's newer copy.
# Only expose libstdc++ — putting all of anaconda/lib on LD_LIBRARY_PATH
# overrides system OpenSSL and breaks ssh-keygen (version mismatch).
RUN mkdir -p /home/ray/lib-overrides && \
    ln -sf /home/ray/anaconda3/lib/libstdc++.so.6 /home/ray/lib-overrides/libstdc++.so.6
ENV LD_LIBRARY_PATH=/home/ray/lib-overrides${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}

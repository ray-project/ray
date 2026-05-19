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

# Force-reinstall nixl-cu13 so its nixl_ep/nixl_ep_cpp.so (linked against
# libcudart.so.13) is the one on disk. nixl-cu12 and nixl-cu13 both ship a
# top-level nixl_ep/ package with identically named binaries; if any prior
# layer left the cu12 binary in site-packages, vLLM's `import nixl_ep` would
# fail with `libcudart.so.12: cannot open shared object file` on cu130.
pip install --no-deps --force-reinstall nixl-cu13==1.1.0

EOF

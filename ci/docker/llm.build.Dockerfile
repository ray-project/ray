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

if [[ "${RAY_CUDA_CODE}" == "cu130" ]]; then
    # nixl-cu12 and nixl-cu13 both own nixl_ep/nixl_ep_cpp.so. Make the CUDA
    # variant deterministic and fail the image build if the cu12 binary is left
    # on disk.
    pip uninstall -y nixl-cu12 nixl-cu13 || true
    pip install --no-cache-dir --no-deps --force-reinstall nixl-cu13==1.1.0
    python - <<'PY'
import importlib.metadata
from pathlib import Path

installed = {dist.metadata["Name"].lower() for dist in importlib.metadata.distributions()}
if "nixl-cu12" in installed:
    raise SystemExit("nixl-cu12 must not be installed in the cu130 LLM image")

site_packages = next(
    Path(path)
    for path in __import__("site").getsitepackages()
    if (Path(path) / "nixl_ep").exists()
)
so = next((site_packages / "nixl_ep").glob("nixl_ep_cpp*.so"))
payload = so.read_bytes()
if b"libcudart.so.12" in payload or b"libcudart.so.13" not in payload:
    raise SystemExit(f"{so} is not the CUDA 13 NIXL EP binary")
print(f"Verified CUDA 13 NIXL EP binary: {so}")
PY
fi

ENV VLLM_USE_FLASHINFER_SAMPLER=0
EOF

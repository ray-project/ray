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
    # nixl-cu12 and nixl-cu13 both install the top-level nixl_ep package.
    # Reinstall cu13 last so vLLM's eager nixl_ep import cannot load a cu12
    # binary from a prior layer or package install.
    pip uninstall -y nixl-cu12 nixl-cu13 || true
    pip install --no-cache-dir --no-deps --force-reinstall nixl-cu13==1.1.0

    python - <<'PY'
import importlib.metadata
import importlib.util
from pathlib import Path

installed = {
    dist.metadata.get("Name", "").lower()
    for dist in importlib.metadata.distributions()
}
if "nixl-cu12" in installed:
    raise SystemExit("nixl-cu12 must not be installed in the CUDA 13 LLM image")
if "nixl" not in installed:
    raise SystemExit("nixl must be installed for vLLM KV connector tests")
if "nixl-cu13" not in installed:
    raise SystemExit("nixl-cu13 must be installed in the CUDA 13 LLM image")

spec = importlib.util.find_spec("nixl_ep")
if spec is None or not spec.submodule_search_locations:
    raise SystemExit("nixl_ep is missing after installing nixl-cu13")

nixl_ep_dir = Path(next(iter(spec.submodule_search_locations)))
candidates = sorted(nixl_ep_dir.glob("nixl_ep_cpp*.so"))
if not candidates:
    raise SystemExit(f"nixl_ep_cpp binary is missing from {nixl_ep_dir}")
so = candidates[0]
payload = so.read_bytes()
if b"libcudart.so.12" in payload or b"libcudart.so.13" not in payload:
    raise SystemExit(f"{so} is not the CUDA 13 NIXL EP binary")

print(f"Verified CUDA 13 NIXL EP binary: {so}")
PY
fi

EOF

ENV VLLM_USE_FLASHINFER_SAMPLER=0

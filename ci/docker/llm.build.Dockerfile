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
    # Keep the NIXL CUDA 13 backend and NIXL EP package available, while
    # removing stale CUDA 12 NIXL files that can be left behind by overlapping
    # nixl-cu12/nixl-cu13 package layouts.
    pip uninstall -y nixl-cu12 nixl-cu13 || true
    pip install --no-cache-dir --no-deps --force-reinstall nixl-cu13==1.1.0

    python - <<'PY'
import importlib.metadata
import importlib.util
import shutil
import site
from pathlib import Path

for site_dir in map(Path, site.getsitepackages()):
    for pattern in (
        "nixl_cu12",
        "nixl_cu12-*",
        "nixl_cu12.libs",
        ".nixl_cu12.*",
    ):
        for path in site_dir.glob(pattern):
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()

importlib.invalidate_caches()

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

if importlib.util.find_spec("nixl") is None:
    raise SystemExit("nixl import spec is missing")
if importlib.util.find_spec("nixl_cu13") is None:
    raise SystemExit("nixl_cu13 import spec is missing")
if importlib.util.find_spec("nixl_ep") is None:
    raise SystemExit("nixl_ep import spec is missing")

nixl_paths = []
for site_dir in map(Path, site.getsitepackages()):
    for pattern in ("nixl_ep", "nixl_cu13", "nixl_cu13.libs", ".nixl_cu13.*"):
        nixl_paths.extend(site_dir.glob(pattern))

bad_paths = []
good_paths = []
for path in nixl_paths:
    files = path.rglob("*") if path.is_dir() else [path]
    for file in files:
        if not file.is_file():
            continue
        payload = file.read_bytes()
        if b"libcudart.so.12" in payload:
            bad_paths.append(str(file))
        if b"libcudart.so.13" in payload:
            good_paths.append(str(file))

if bad_paths:
    raise SystemExit(f"CUDA 12 NIXL files found in CUDA 13 LLM image: {bad_paths}")
if not good_paths:
    raise SystemExit("No CUDA 13 NIXL binaries found in the CUDA 13 LLM image")

print("Verified CUDA 13 NIXL backend with nixl_ep enabled")
PY
fi

EOF

ENV VLLM_USE_FLASHINFER_SAMPLER=0

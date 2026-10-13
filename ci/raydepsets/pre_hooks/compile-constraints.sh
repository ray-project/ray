#!/bin/bash
#
# Compile python/requirements_compiled.txt, the constraints file that every depset
# reads, with a universal uv resolution. One file covers every supported Python and
# platform: where a version must differ, uv writes one line per range with a marker.
# uv prefers the pins already in the output file, so a recompile only moves what the
# source requirements force.
#
# raydepsets sets RAYDEPSETS_UV to its pinned uv. Outside raydepsets, put that same
# uv version on PATH.

set -euo pipefail

UV="${RAYDEPSETS_UV:-uv}"
TARGET="python/requirements_compiled.txt"

# Keep in sync with COMPILE_PIP_SOURCES in ci/ci.sh, which still compiles
# requirements_compiled_py3.14.txt.
SOURCES=(
  python/requirements.txt
  python/requirements/lint-requirements.txt
  python/requirements/test-requirements.txt
  python/requirements/cloud-requirements.txt
  python/requirements/serve/serve-test-requirements.txt
  python/requirements/docker/ray-docker-requirements.txt
  python/requirements/ml/core-requirements.txt
  python/requirements/ml/data-requirements.txt
  python/requirements/ml/data-test-requirements.txt
  python/requirements/ml/dl-cpu-requirements.txt
  python/requirements/ml/ml-requirements.txt
  python/requirements/ml/third_party.txt
  python/requirements/ml/rllib-requirements.txt
  python/requirements/ml/rllib-test-requirements.txt
  python/requirements/ml/train-requirements.txt
  python/requirements/ml/train-test-requirements.txt
  python/requirements/ml/tune-requirements.txt
  python/requirements/ml/tune-test-requirements.txt
  python/requirements/security-requirements.txt
)

# uv writes whatever index it used into the lockfile. Keep CI's temporary mirror
# URL out, or the regenerated file stops matching the committed one.
env -u PIP_INDEX_URL -u PIP_EXTRA_INDEX_URL -u PIP_TRUSTED_HOST \
  -u UV_INDEX_URL -u UV_EXTRA_INDEX_URL -u UV_INSECURE_HOST \
  "${UV}" pip compile --universal --python-version 3.10 \
  --quiet --no-header --strip-extras \
  --emit-index-url --emit-find-links \
  --index-strategy unsafe-best-match \
  --unsafe-package ray \
  --unsafe-package pip \
  --unsafe-package setuptools \
  -o "${TARGET}" \
  "${SOURCES[@]}"

sed -i -e "/^--index-url /d" -e "/@ file/d" "${TARGET}"

# Strip the local version segment from CPU builds (+cpu, +pt210cpu,
# +cpu.torch.2.10). The GPU depsets also read this file, and only a bare
# torch-scatter==2.1.2 lets their +cu.12.8.torch.2.10 pin satisfy it. The
# public version may carry a pre/post/dev suffix (2.10.0rc1, 2.10.0.post0),
# hence letters in the first class.
sed -i -E 's/==([A-Za-z0-9.]+)[+][A-Za-z0-9._-]*cpu[A-Za-z0-9._-]*/==\1/g' "${TARGET}"

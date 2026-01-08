# syntax=docker/dockerfile:1.3-labs
#
# Ray Image Builder
# =================
# Installs the Ray wheel into a base image (CPU or CUDA).
#
# This Dockerfile uses multi-stage builds to:
# 1. Extract the wheel from the ray-wheel wanda cache (scratch image)
# 2. Install it into the base image (ray-py{VER}-{cpu/cuda}-base)
#
# The base image already contains:
# - Python with conda/anaconda
# - Core dependencies (numpy, etc.)
# - System libraries (jemalloc, etc.)
#
# This image adds:
# - Ray wheel with [all] extras
# - pip freeze output for reproducibility
#
ARG BASE_IMAGE
ARG RAY_WHEEL_IMAGE

FROM ${RAY_WHEEL_IMAGE} AS wheel-source
FROM ${BASE_IMAGE}

ARG PYTHON_VERSION=3.10

COPY --from=wheel-source /*.whl /tmp/
COPY python/requirements_compiled.txt /tmp/

# Install Ray wheel with all extras
RUN <<EOF
#!/bin/bash
set -euo pipefail

# Find the wheel file
WHEEL_FILES=(/tmp/ray-*.whl)
if [[ ${#WHEEL_FILES[@]} -ne 1 ]]; then
    echo "Error: Expected 1 ray wheel file, but found ${#WHEEL_FILES[@]} in /tmp/." >&2
    ls -l /tmp/*.whl >&2
    exit 1
fi
WHEEL_FILE="${WHEEL_FILES[0]}"

echo "Installing wheel: $WHEEL_FILE"

# Install ray with all extras, using constraints for reproducibility
$HOME/anaconda3/bin/pip --no-cache-dir install \
    -c /tmp/requirements_compiled.txt \
    "${WHEEL_FILE}[all]"

# Save pip freeze for debugging/reproducibility
$HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

echo "Ray version: $($HOME/anaconda3/bin/python -c 'import ray; print(ray.__version__)')"
EOF

CMD ["python"]

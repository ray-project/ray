# syntax=docker/dockerfile:1.3-labs
#
# Ray Image Builder
# ==============================
# Installs the Ray wheel into a base image (CPU or CUDA), includes
# pip freeze output for reproducibility.
#
ARG PYTHON_VERSION=3.10
ARG PLATFORM=cpu
ARG ARCH_SUFFIX=
ARG IMAGE_TYPE=ray
ARG BASE_VARIANT=base
ARG BASE_IMAGE=cr.ray.io/rayproject/${IMAGE_TYPE}-py${PYTHON_VERSION}-${PLATFORM}-${BASE_VARIANT}${ARCH_SUFFIX}
ARG RAY_WHEEL_IMAGE=cr.ray.io/rayproject/ray-wheel-py${PYTHON_VERSION}${ARCH_SUFFIX}

FROM ${RAY_WHEEL_IMAGE} AS wheel-source
FROM ${BASE_IMAGE}

ARG RAY_COMMIT=unknown-commit
ARG RAY_VERSION=3.0.0.dev0

LABEL io.ray.ray-commit="${RAY_COMMIT}"
LABEL io.ray.ray-version="${RAY_VERSION}"

COPY --from=wheel-source /*.whl /home/ray/

# Install Ray wheel with all extras
# Uses requirements_compiled.txt from base image (already at /home/ray/)
RUN <<EOF
#!/bin/bash
set -euo pipefail

WHEEL_FILES=(/home/ray/ray-*.whl)
if [[ ${#WHEEL_FILES[@]} -ne 1 ]]; then
    echo "Error: Expected 1 ray wheel file, but found ${#WHEEL_FILES[@]} in /home/ray/." >&2
    ls -l /home/ray/*.whl >&2
    exit 1
fi
WHEEL_FILE="${WHEEL_FILES[0]}"

echo "Installing wheel: $WHEEL_FILE"

$HOME/anaconda3/bin/pip --no-cache-dir install \
    -c /home/ray/requirements_compiled.txt \
    "${WHEEL_FILE}[all]"

$HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

echo "Ray version: $($HOME/anaconda3/bin/python -c 'import ray; print(ray.__version__)')"
EOF

CMD ["/bin/bash"]

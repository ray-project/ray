# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_gpu-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON
ARG PYTHON_DEPSET=python/deplocks/ci/docgpu_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

# --no-binary deepspeed: the GPU locks resolve against Astral's cu128 index, which publishes prebuilt deepspeed wheels
#   as local versions (0.18.9+cu.12.8.torch.2.11), however, currently we use torch 2.9 which is incompatible
uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match --no-binary deepspeed

# Remove installed ray so the source overlay at /rayci/ is used at test time
pip uninstall -y ray

EOF

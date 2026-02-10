# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_gpu-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD
SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

DOC_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 ci/env/install-dependencies.sh
pip install -Ur ./python/requirements/ml/dl-gpu-requirements.txt

EOF

# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_gpu
FROM $DOCKER_IMAGE_BASE_BUILD

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

pip install -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt

pip install vllm==0.4.2 transformers==4.40.0 torch==2.3.0+cu118 \
  torchvision==0.18.0+cu118 torch-scatter==2.1.2+pt23cu118 \
  torch-sparse==0.6.18+pt23cu118 torch-cluster==1.6.3+pt23cu118 \
  torch-spline-conv==1.2.2+pt23cu118 \
  --extra-index-url https://download.pytorch.org/whl/cu118  \
  --find-links https://data.pyg.org/whl/torch-2.3.0+cu118.html 

EOF


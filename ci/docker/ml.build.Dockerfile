# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAYCI_IS_GPU_BUILD=false
ARG RAYCI_LIGHTNING_2=false

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

DOC_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 DATA_PROCESSING_TESTING=1 INSTALL_HDFS=1 \
  ./ci/env/install-dependencies.sh

if [[ "$RAYCI_IS_GPU_BUILD" == "true" ]]; then
  pip install -Ur ./python/requirements/ml/dl-gpu-requirements.txt
fi

if [[ "$RAYCI_LIGHTNING_2" == "true" ]]; then
  pip uninstall -y pytorch-lightning
  # todo move to requirements-test.txt
  pip install lightning==2.1.2 pytorch-lightning==2.1.2
fi

# Remove installed ray.
pip uninstall -y ray

EOF

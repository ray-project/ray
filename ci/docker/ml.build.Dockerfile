# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAYCI_IS_GPU_BUILD=false
ARG RAYCI_LIGHTNING_2=false
ARG PYTHON

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

set -x

if [[ "${PYTHON-}" == "3.12" ]]; then
  # hebo and doc test dependencies are not needed for 3.12 test jobs
  TRAIN_TESTING=1 TUNE_TESTING=1 DATA_PROCESSING_TESTING=1 \
    INSTALL_HDFS=1 ./ci/env/install-dependencies.sh
else
  DOC_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 DATA_PROCESSING_TESTING=1 \
    INSTALL_HDFS=1 ./ci/env/install-dependencies.sh

  pip install HEBO==0.3.5
fi

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

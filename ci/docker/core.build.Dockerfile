ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAYCI_IS_GPU_BUILD=false

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

pip install -U --ignore-installed  \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt \
  -r python/requirements/ml/dl-cpu-requirements.txt

if [[ "$RAYCI_IS_GPU_BUILD" == "true" ]]; then
  pip install -Ur ./python/requirements/ml/dl-gpu-requirements.txt
fi

EOF

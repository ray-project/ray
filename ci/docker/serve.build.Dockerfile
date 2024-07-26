# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYDANTIC_VERSION
ARG PYTHON

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

pip install -U --ignore-installed \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt

# TODO(can): upgrade tensorflow for python 3.12
if [[ "${PYTHON-}" != "3.12" ]]; then
  pip install -U -c python/requirements_compiled.txt \
    tensorflow tensorflow-probability torch torchvision \
    transformers aioboto3
fi
git clone https://github.com/wg/wrk.git /tmp/wrk && pushd /tmp/wrk && make -j && sudo cp wrk /usr/local/bin && popd

# Install custom Pydantic version if requested.
if [[ -n "${PYDANTIC_VERSION-}" ]]; then 
  pip install -U pydantic==$PYDANTIC_VERSION
else 
  echo "Not installing Pydantic from source"
fi

EOF


# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ENABLE_TRACING
ARG PYDANTIC_VERSION
ARG PYTHON

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

git clone --branch=4.2.0 --depth=1 https://github.com/wg/wrk.git /tmp/wrk
make -C /tmp/wrk -j
sudo cp /tmp/wrk/wrk /usr/local/bin/wrk
rm -rf /tmp/wrk

# Install custom Pydantic version if requested.
if [[ -n "${PYDANTIC_VERSION-}" ]]; then
  pip install -U pydantic==$PYDANTIC_VERSION
else
  echo "Not installing Pydantic from source"
fi

if [[ "${ENABLE_TRACING-}" == "1" ]]; then
  # Install tracing dependencies if requested. Intentionally, we do not use
  # requirements_compiled.txt as the constraint file. They are not compatible with
  # a few packages in that file (e.g. requiring an ugprade to protobuf 5+).
  pip install opentelemetry-exporter-otlp==1.34.1
else
  echo "Not installing tracing dependencies"
fi

EOF

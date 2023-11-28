# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON_VERSION
ARG PYDANTIC_VERSION

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

# Install podman
RUN sudo apt install software-properties-common uidmap -y && \
    sudo sh -c "echo 'deb http://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_20.04/ /' > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list" && \
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 4D64390375060AA4 && \
    sudo apt-get update && \
    sudo apt-get install podman -y

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

# Install custom Python version if requested.
if [[ -n "${PYTHON_VERSION-}" ]]; then 
  PYTHON=$PYTHON_VERSION ci/env/install-dependencies.sh 
else 
  echo "Not installing custom Python version" 
fi

pip install -U torch==2.0.1 torchvision==0.15.2
pip install -U tensorflow==2.13.1 tensorflow-probability==0.21.0
pip install -U --ignore-installed \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt

# doc requirements
#
# TODO (shrekris-anyscale): Remove transformers after core transformer requirement 
# is upgraded
pip install transformers==4.30.2
pip install -c python/requirements_compiled.txt aioboto3

git clone https://github.com/wg/wrk.git /tmp/wrk && pushd /tmp/wrk && make -j && sudo cp wrk /usr/local/bin && popd

# Install custom Pydantic version if requested.
if [[ -n "${PYDANTIC_VERSION-}" ]]; then 
  pip install -U pydantic==$PYDANTIC_VERSION
else 
  echo "Not installing Pydantic from source"
fi

EOF


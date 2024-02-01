# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON_VERSION
ARG EXTRA_DEPENDENCY

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

# minimal dependencies
MINIMAL_INSTALL=1 PYTHON=${PYTHON_VERSION} ci/env/install-dependencies.sh
rm -rf python/ray/thirdparty_files

# install test requirements
python -m pip install -U pytest==7.0.1

# install extra dependencies
if [[ "${EXTRA_DEPENDENCY}" == "core" ]]; then
  ./ci/env/install-core-prerelease-dependencies.sh
elif [[ "${EXTRA_DEPENDENCY}" == "ml" ]]; then 
  python -m pip install -U "ray[tune]"
elif [[ "${EXTRA_DEPENDENCY}" == "default" ]]; then
  python -m pip install -U "ray[default]"
elif [[ "${EXTRA_DEPENDENCY}" == "serve" ]]; then
  python -m pip install -U "ray[serve]"
fi

EOF




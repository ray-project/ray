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
python -m pip install -U pytest==7.0.1 pip-tools==7.3.0

# install extra dependencies
if [[ "${EXTRA_DEPENDENCY}" == "core" ]]; then
  ./ci/env/install-core-prerelease-dependencies.sh
elif [[ "${EXTRA_DEPENDENCY}" == "ml" ]]; then
  pip-compile -o min_requirements.txt python/setup.py --extra tune
elif [[ "${EXTRA_DEPENDENCY}" == "default" ]]; then
  pip-compile -o min_requirements.txt python/setup.py --extra default
elif [[ "${EXTRA_DEPENDENCY}" == "serve" ]]; then
  pip-compile -o min_requirements.txt python/setup.py --extra serve-grpc
fi

if [[ -f min_requirements.txt ]]; then
  pip install -r min_requirements.txt
fi

EOF

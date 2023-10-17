ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON_VERSION

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

# minimal dependencies
RUN MINIMAL_INSTALL=1 PYTHON=${PYTHON_VERSION} ci/env/install-dependencies.sh
RUN rm -rf python/ray/thirdparty_files

# install test requirements
RUN python -m pip install -U pytest==7.0.1 numpy

# core pre-release denpendencies
RUN ./ci/env/install-core-prerelease-dependencies.sh

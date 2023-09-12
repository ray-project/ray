ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
ARG ARROW_VERSION
FROM $DOCKER_IMAGE_BASE_BUILD

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN DATA_PROCESSING_TESTING=1 ARROW_VERSION=$ARROW_VERSION ./ci/env/install-dependencies.sh
RUN pip install "datasets==2.14.0"

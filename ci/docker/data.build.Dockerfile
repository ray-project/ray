# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_VERSION=14.*
ARG ARROW_MONGO_VERSION=
ARG RAY_CI_JAVA_BUILD=

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

DATA_PROCESSING_TESTING=1 ARROW_VERSION=$ARROW_VERSION \
  ARROW_MONGO_VERSION=$ARROW_MONGO_VERSION ./ci/env/install-dependencies.sh

# Install MongoDB
sudo apt-get purge -y mongodb*
sudo apt-get install -y mongodb
sudo rm -rf /var/lib/mongodb/mongod.lock

if [[ $RAY_CI_JAVA_BUILD == 1 ]]; then
  # These packages increase the image size quite a bit, so we only install them 
  # as needed.
  sudo apt-get install -y -qq maven openjdk-8-jre openjdk-8-jdk
fi

EOF

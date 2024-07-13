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

set -ex

DATA_PROCESSING_TESTING=1 ARROW_VERSION=$ARROW_VERSION \
  ARROW_MONGO_VERSION=$ARROW_MONGO_VERSION ./ci/env/install-dependencies.sh
if [[ -n "$ARROW_MONGO_VERSION" ]]; then
  # Older versions of Arrow Mongo require an older version of NumPy.
  pip install numpy==1.23.5
fi

# Install MongoDB
curl -fsSL https://pgp.mongodb.com/server-7.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org

if [[ $RAY_CI_JAVA_BUILD == 1 ]]; then
  # These packages increase the image size quite a bit, so we only install them 
  # as needed.
  sudo apt-get install -y -qq maven openjdk-8-jre openjdk-8-jdk
fi

EOF

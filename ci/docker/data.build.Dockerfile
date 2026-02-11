# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_VERSION=23.*
ARG ARROW_MONGO_VERSION=
ARG RAY_CI_JAVA_BUILD=

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

curl -fsSL https://pgp.mongodb.com/server-8.0.asc | \
  sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] \
  https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org

if [[ $RAY_CI_JAVA_BUILD == 1 ]]; then
  # These packages increase the image size quite a bit, so we only install them
  # as needed.
  sudo apt-get install -y -qq maven openjdk-8-jre openjdk-8-jdk
fi

EOF

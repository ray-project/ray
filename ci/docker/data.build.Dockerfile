# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_MONGO_VERSION=
ARG RAY_CI_JAVA_BUILD=
ARG IMAGE_TYPE=base
ARG PYTHON_DEPSET=python/deplocks/ci/data-$IMAGE_TYPE-ci_depset_py$PYTHON.lock

COPY $PYTHON_DEPSET /home/ray/python_depset.lock

SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash

set -ex

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

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

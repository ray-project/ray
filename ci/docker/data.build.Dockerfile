# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAY_CI_JAVA_BUILD=
ARG IMAGE_TYPE=base
ARG PYTHON=3.10
ARG PYTHON_DEPSET=python/deplocks/ci/data-$IMAGE_TYPE-ci_depset_py$PYTHON.lock

COPY $PYTHON_DEPSET /home/ray/python_depset.lock

SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash

set -ex

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

if [[ "$IMAGE_TYPE" == "pyarrow-nightly" ]]; then
  uv pip install \
    --system \
    --pre \
    --prefer-binary \
    --extra-index-url https://pypi.fury.io/arrow-nightlies/ \
    pyarrow
fi

curl -fsSL https://pgp.mongodb.com/server-8.0.asc | \
  sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] \
  https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org

if [[ $RAY_CI_JAVA_BUILD == 1 ]]; then
  # These packages increase the image size quite a bit, so we only install them
  # as needed.
  sudo apt-get install -y -qq maven openjdk-8-jre openjdk-8-jdk
  # Ensure Java 8 is the default; Ubuntu 22.04 defaults to Java 11 which
  # breaks Spark's reflective access to DirectByteBuffer.
  if [[ "$(dpkg --print-architecture)" == "arm64" ]]; then
      sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-arm64/jre/bin/java
  else
      sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
  fi
fi

EOF

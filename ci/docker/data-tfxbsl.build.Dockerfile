# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_VERSION=14.*
ARG ARROW_MONGO_VERSION=
ARG RAY_CI_JAVA_BUILD=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

ARROW_VERSION=$ARROW_VERSION ./ci/env/install-dependencies.sh
# We manually install tfx-bsl here. Adding the library via data- or
# test-requirements.txt files causes unresolvable dependency conflicts with pandas.

pip install -U tfx-bsl==1.16.1 crc32c==2.3

EOF

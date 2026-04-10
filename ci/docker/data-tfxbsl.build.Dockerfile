# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_VERSION=14.*
ARG ARROW_MONGO_VERSION=
ARG RAY_CI_JAVA_BUILD=
ARG PYTHON_DEPSET=python/deplocks/ci/data-tfxbsl-ci_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

ARROW_VERSION=$ARROW_VERSION ./ci/env/install-dependencies.sh
# We manually install tfx-bsl here. Adding the library via data- or
# test-requirements.txt files causes unresolvable dependency conflicts with pandas.

pip install --no-deps tfx-bsl==1.16.1
pip install -U crc32c==2.3 "apache-beam[gcp]==2.59.0" protobuf==4.25.8 googleapis-common-protos==1.66.0 grpcio==1.62.3 google-auth==2.49.0

EOF

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
# We manually install tensorflow-datasets and tensorflow here. Adding the library via dl-cpu-requirements.txt or
# dl-gpu-requirements.txt files causes unresolvable dependency conflicts with protobuf for python < 3.11

pip install tensorflow-datasets==4.9.9 tensorflow==2.20.0

EOF

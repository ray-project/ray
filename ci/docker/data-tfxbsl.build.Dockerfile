# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_VERSION=14.*
ARG ARROW_MONGO_VERSION=
ARG RAY_CI_JAVA_BUILD=

SHELL ["/bin/bash", "-ce"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

pip install -r python/requirements_datatfxbslbuild_py310.txt

EOF

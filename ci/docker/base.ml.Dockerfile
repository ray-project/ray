# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_TEST=cr.ray.io/rayproject/oss-ci-base_test
FROM $DOCKER_IMAGE_BASE_TEST

COPY . .

RUN <<EOF
#!/bin/bash -i

set -e

BUILD=1 ./ci/ci.sh init
RLLIB_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 bash --login -i ./ci/env/install-dependencies.sh

pip uninstall -y ray

EOF

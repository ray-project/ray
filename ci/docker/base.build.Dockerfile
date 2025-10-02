ARG DOCKER_IMAGE_BASE_TEST=cr.ray.io/rayproject/oss-ci-base_test
FROM $DOCKER_IMAGE_BASE_TEST

COPY . .

RUN <<EOF
#!/bin/bash -i

set -euo pipefail

apt-get update -y
apt-get install -y -qq maven openjdk-8-jre openjdk-8-jdk

BUILD=1 ./ci/ci.sh init

EOF

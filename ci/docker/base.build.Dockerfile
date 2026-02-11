ARG DOCKER_IMAGE_BASE_TEST=cr.ray.io/rayproject/oss-ci-base_test
FROM $DOCKER_IMAGE_BASE_TEST

ARG RAYCI_DISABLE_JAVA=false

COPY . .

RUN <<EOF
#!/bin/bash -i

set -euo pipefail

if [[ "$RAYCI_DISABLE_JAVA" != "true" ]]; then
    apt-get update -y
    apt-get install -y -qq maven openjdk-8-jre openjdk-8-jdk
    # Ensure Java 8 is the default; Ubuntu 24.04 defaults to Java 21.
    update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
fi

BUILD=1 ./ci/ci.sh init

EOF

ENV CC=clang
ENV CXX=clang++-14

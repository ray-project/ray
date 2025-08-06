ARG DOCKER_IMAGE_BASE_TEST=cr.ray.io/rayproject/oss-ci-base_test
FROM $DOCKER_IMAGE_BASE_TEST

ARG RAY_INSTALL_JAVA=true
ENV RAY_INSTALL_JAVA=$RAY_INSTALL_JAVA

COPY . .

RUN <<EOF
#!/bin/bash -i

set -euo pipefail

apt-get update -y

if [[ "$RAY_INSTALL_JAVA" == "1" || "$RAY_INSTALL_JAVA" == "true" ]]; then
  echo "Installing OpenJDK + Maven..."
  apt-get install -y -qq maven openjdk-8-jre openjdk-8-jdk
else
  echo "Skipping JDK and Maven installation (RAY_INSTALL_JAVA=$RAY_INSTALL_JAVA)"
fi

BUILD=1 ./ci/ci.sh init

EOF

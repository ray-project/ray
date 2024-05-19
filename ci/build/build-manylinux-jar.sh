#!/bin/bash
set -exuo pipefail

if [[ "${RAY_INSTALL_JAVA}" == "1" ]]; then
  if [[ "${BUILD_JAR-}" == "1" ]]; then
    echo "--- Build JAR"
    ./java/build-jar-multiplatform.sh linux
  fi
fi

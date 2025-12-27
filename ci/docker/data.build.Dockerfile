# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_test-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_VERSION=20.*
ARG ARROW_MONGO_VERSION=
ARG RAY_CI_JAVA_BUILD=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -ex

./ci/ci.sh configure_system
./ci/env/install-dependencies.sh install_base
./ci/env/install-dependencies.sh install_toolchains
./ci/env/install-dependencies.sh install_thirdparty_packages

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"
uv pip install --system -r ./python/requirements/ml/data-requirements.txt -c ./python/requirements_compiled.txt

rm -rf /opt/miniforge/pkgs/cache/
rm /root/Miniforge3-25.3.0-1-Linux-x86_64.sh

EOF

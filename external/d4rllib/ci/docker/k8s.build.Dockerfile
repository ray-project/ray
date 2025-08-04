# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/oss-ci-base_build

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

bash ci/k8s/install-k8s-tools.sh

pip install -U --ignore-installed \
    -r python/requirements.txt -c python/requirements_compiled.txt \

pip install -U --ignore-installed \
    docker -c python/requirements_compiled.txt \

EOF

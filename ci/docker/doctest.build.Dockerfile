ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py$PYTHON
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON=3.10
ARG PYTHON_DEPSET=python/deplocks/docs/doctest_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY . .
COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN  <<EOF
#!/bin/bash

set -euo pipefail

sudo apt-get update
sudo apt-get install -y tesseract-ocr libosmesa6-dev libglfw3 patchelf

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

EOF

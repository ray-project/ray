# syntax=docker/dockerfile:1.3-labs

# The "docstring-only" label's examples environment: ray[all] and the libraries
# that modules holding >>> examples import, without torch or TensorFlow. It
# builds on oss-ci-base_test rather than an ML base image so it's small to
# pull. See docstring_examples_depset in ci/raydepsets/configs/docs.depsets.yaml.

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_test-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON=3.10
ARG PYTHON_DEPSET=python/deplocks/docs/docstring_examples_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

# oss-ci-base_test doesn't ship uv; oss-ci-base_build gets it from ci.sh init.
curl -fsSL https://astral.sh/uv/0.11.33/install.sh | env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

EOF

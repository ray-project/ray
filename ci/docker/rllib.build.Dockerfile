ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAYCI_IS_GPU_BUILD
ARG PYTHON
ARG BUILD_VARIANT=build
ARG PYTHON_DEPSET=python/deplocks/ci/rllib-$BUILD_VARIANT-ci_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py$PYTHON
FROM $DOCKER_IMAGE_BASE_BUILD

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN DOC_TESTING=1 ./ci/env/install-dependencies.sh

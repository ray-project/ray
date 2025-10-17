ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

SHELL ["/bin/bash", "-ice"]
ARG PIP_REQUIREMENTS

COPY $PIP_REQUIREMENTS python_depset.lock

RUN pip install -r python_depset.lock

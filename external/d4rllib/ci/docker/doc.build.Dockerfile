ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN pip install -r doc/requirements-doc.txt

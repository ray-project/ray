ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

SHELL ["/bin/bash", "-ice"]

RUN curl -LsSf https://astral.sh/uv/0.8.19/install.sh | sh

COPY . .

RUN pip install -r doc/requirements-doc.txt

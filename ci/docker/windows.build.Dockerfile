ARG DOCKER_IMAGE_BASE_BUILD=rayproject/buildenv:windows
FROM $DOCKER_IMAGE_BASE_BUILD
ARG PYTHON=3.9
ENV PYTHON=$PYTHON

COPY . .
RUN bash ci/ray_ci/windows/build_base.sh

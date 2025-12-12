ARG DOCKER_IMAGE_BASE_BUILD=rayproject/buildenv:windows
FROM $DOCKER_IMAGE_BASE_BUILD

ENV PYTHON=3.10
ENV RAY_BUILD_ENV=win_py$PYTHON

COPY . .
RUN bash ci/ray_ci/windows/build_base.sh

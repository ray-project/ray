ARG DOCKER_IMAGE_BASE_BUILD=rayproject/buildenv:windows
FROM $DOCKER_IMAGE_BASE_BUILD

COPY . .
RUN bash verify-windows-wheels.sh

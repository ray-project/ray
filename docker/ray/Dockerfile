# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray-deps:nightly"$BASE_IMAGE"
FROM $FULL_BASE_IMAGE

ARG PYTHON_VERSION=3.11
ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
ARG CONSTRAINTS_FILE="requirements_compiled.txt"
ARG LOCK_FILE="requirements_compiled_ray_img_${PYTHON_VERSION}.txt"

COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH
COPY $LOCK_FILE $LOCK_FILE

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -r $LOCK_FILE --no-deps

RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

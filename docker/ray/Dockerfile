# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray-deps:nightly"$BASE_IMAGE"
FROM $FULL_BASE_IMAGE

ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
ARG CONSTRAINTS_FILE

COPY $WHEEL_PATH .
COPY $CONSTRAINTS_FILE .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -c $CONSTRAINTS_FILE \
    `basename $WHEEL_PATH`[all] \
    --find-links $FIND_LINKS_PATH && sudo rm `basename $WHEEL_PATH`

RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

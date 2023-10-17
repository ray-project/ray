# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray-deps:nightly"$BASE_IMAGE"
FROM $FULL_BASE_IMAGE

ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
ARG CONSTRAINTS_FILE="requirements_compiled.txt"

COPY requirements_compiled.txt ./
COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH

RUN <<EOF
#!/bin/bash

set -euo pipefail

WHEEL_NAME="$(basename $WHEEL_PATH)"

$HOME/anaconda3/bin/pip --no-cache-dir \
    install -c $CONSTRAINTS_FILE "${WHEEL_NAME}" \
    --find-links $FIND_LINKS_PATH

sudo rm "${WHEEL_NAME}"

EOF

RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

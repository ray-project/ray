# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray-deps:nightly"$BASE_IMAGE"
FROM $FULL_BASE_IMAGE

ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
ARG CONSTRAINTS_FILE="requirements_compiled.txt"
ARG LOCK_FILE="python/ray-img/requirements_compiled_ray_img_*.txt"

COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH
COPY $LOCK_FILE $LOCK_FILE

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -c $CONSTRAINTS_FILE \
    `basename $WHEEL_PATH`[all] \
    --find-links $FIND_LINKS_PATH && sudo rm `basename $WHEEL_PATH`


RUN <<EOF
#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"

$HOME/anaconda3/bin/pip --no-cache-dir install $LOCK_FILE --no-deps
EOF


RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

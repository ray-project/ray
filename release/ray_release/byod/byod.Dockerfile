# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG PIP_REQUIREMENTS
ARG DEBIAN_REQUIREMENTS

COPY "$DEBIAN_REQUIREMENTS" .
RUN <<EOF
#!/bin/bash

sudo apt-get update -y \
    && sudo apt-get install -y --no-install-recommends $(cat requirements_debian_byod.txt) \
    && sudo apt-get autoclean

rm -rf /tmp/wrk
git clone --branch 4.2.0 https://github.com/wg/wrk.git /tmp/wrk
make -C /tmp/wrk -j
sudo cp /tmp/wrk/wrk /usr/local/bin/wrk

EOF

COPY "$PIP_REQUIREMENTS" .
RUN "$HOME"/anaconda3/bin/pip install --no-cache-dir https://ray-ci-deps-wheels.s3.us-west-2.amazonaws.com/AutoROM.accept_rom_license-0.5.4-py3-none-any.whl
RUN "$HOME"/anaconda3/bin/pip install --no-cache-dir -r "${PIP_REQUIREMENTS}"

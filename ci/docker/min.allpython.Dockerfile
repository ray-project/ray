# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

ARG DEFAULT_PYTHON_VERSION=3.9

ARG UV_BIN=/usr/local/bin/uv

ARG UV_PYTHON_INSTALL_DIR=~/local/python

SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash

set -euo pipefail

# Install Python versions
"${UV_BIN}" python install 3.9 3.10 3.11 3.12 3.13

# Set default Python version
"${UV_BIN}" python pin "${DEFAULT_PYTHON_VERSION}"

EOF

# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

ARG DEFAULT_PYTHON_VERSION=3.9
ARG UV_BIN=/usr/local/bin/uv
SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash

set -euo pipefail

# Install Python versions
sudo -n "${UV_BIN}" python install 3.9 --preview-features python-install-default
sudo -n "${UV_BIN}" python install 3.10 --preview-features python-install-default
sudo -n "${UV_BIN}" python install 3.11 --preview-features python-install-default
sudo -n "${UV_BIN}" python install 3.12 --preview-features python-install-default
sudo -n "${UV_BIN}" python install 3.13 --preview-features python-install-default

# Set default Python version
sudo -n "${UV_BIN}" python pin "${DEFAULT_PYTHON_VERSION}"

EOF

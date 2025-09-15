FROM cr.ray.io/rayproject/forge

ARG DEFAULT_PYTHON_VERSION=3.9
ARG BIN_PATH=/home/forge/.local/bin
ENV PATH="${BIN_PATH}:${PATH}"
ARG UV_BIN="${BIN_PATH}"/uv

# # install uv
# RUN curl -LsSf https://astral.sh/uv/0.8.17/install.sh |  env UV_INSTALL_DIR="${BIN_PATH}" sh


SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash

set -euo pipefail

# Install Python versions
"${UV_BIN}" python install 3.9 3.10 3.11 3.12 3.13

# Set default Python version
"${UV_BIN}" python pin 3.9

EOF

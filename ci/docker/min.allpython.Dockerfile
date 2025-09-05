# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

ARG DEFAULT_PYTHON_VERSION=3.9

# create a writable dir for the forge user
RUN mkdir -p /home/forge/app && chown -R forge /home/forge

WORKDIR /home/forge/app

SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash

set -euo pipefail

# Install uv
curl -sSL -o- https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="/usr/local/bin" sh

# Install Python versions
uv python install 3.9 3.10 3.11 3.12 3.13

# Set default Python version
uv python pin ${DEFAULT_PYTHON_VERSION}

EOF

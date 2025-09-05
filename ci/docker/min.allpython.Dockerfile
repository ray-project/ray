# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

ARG DEFAULT_PYTHON_VERSION=3.9

USER root
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
USER forge

# create a writable dir for the forge user
RUN mkdir -p /home/forge/app && chown -R forge /home/forge

WORKDIR /home/forge/app

SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash

set -euo pipefail

# Install Python versions
uv python install 3.9 3.10 3.11 3.12 3.13

# Set default Python version
uv python pin ${DEFAULT_PYTHON_VERSION}

EOF

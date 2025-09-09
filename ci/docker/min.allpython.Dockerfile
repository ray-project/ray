# syntax=docker/dockerfile:1.3-labs

FROM quay.io/pypa/manylinux2014_x86_64:2024-07-02-9ac04ee

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

# install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.local/bin/env

EOF

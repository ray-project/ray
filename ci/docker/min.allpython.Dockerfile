# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

ARG DEFAULT_PYTHON_VERSION=3.9

SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash

set -euo pipefail

useradd -ms /bin/bash -d /home/ray ray --uid 1000 --gid 100
usermod -aG sudo ray
echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers

# Install uv
curl -LsSf https://astral.sh/uv/0.8.14/install.sh | sh

# Install Python versions
uv python install 3.9 3.10 3.11 3.12 3.13

# Set default Python version
uv python pin ${DEFAULT_PYTHON_VERSION}

EOF

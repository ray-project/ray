# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

ARG DEFAULT_PYTHON_VERSION=3.9

# ARG UV_BIN=/usr/local/bin/uv

# uv-managed install location (user-owned)
ENV XDG_DATA_HOME=/home/forge/.local/share
ENV UV_PYTHON_INSTALL_DIR=${XDG_DATA_HOME}/uv/python
ENV PATH=/home/forge/.local/bin:${PATH}

# Make dirs and install uv for the user
RUN set -eux; mkdir -p "$UV_PYTHON_INSTALL_DIR" /home/forge/.local/bin \
  && curl -fsSL https://astral.sh/uv/install.sh | sh

# Install Default Pythons and expose via symlinks in ~/.local/bin
RUN set -eux; \
  python install 3.9 3.10 3.11 3.12 3.13 \
  pybin="$(uv python find ${DEFAULT_PYTHON_VERSION})/bin"; \
  ln -sf "${pybin}/python3" /home/forge/.local/bin/python3; \
  ln -sf "${pybin}/pip3"    /home/forge/.local/bin/pip3

SHELL ["/bin/bash", "-ice"]

RUN python pin "${DEFAULT_PYTHON_VERSION}"

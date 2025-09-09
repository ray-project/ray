# syntax=docker/dockerfile:1.3-labs

FROM quay.io/pypa/manylinux2014_x86_64:2024-07-02-9ac04ee

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
&& source ~/.local/bin/env

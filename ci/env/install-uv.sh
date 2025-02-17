#!/usr/bin/env bash

curl -LsSf https://astral.sh/uv/0.6.0/install.sh | sh
uv python install "${PYTHON}"

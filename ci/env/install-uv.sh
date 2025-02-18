#!/usr/bin/env bash

curl -LsSf https://astral.sh/uv/0.6.0/install.sh | sh
source $HOME/.local/bin/env
uv python install "${PYTHON}"

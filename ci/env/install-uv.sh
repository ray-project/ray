#!/usr/bin/env bash

curl -LsSf https://astral.sh/uv/0.6.1/install.sh | env UV_UNMANAGED_INSTALL="$HOME/.local/bin" sh
$HOME/.local/bin/uv python install "${PYTHON}" --default --preview
$HOME/.local/bin/uv tool update-shell

#!/bin/bash
set -exuo pipefail

PYTHON="$1"

if [[ ! "${OSTYPE}" =~ ^linux ]]; then
  echo "ERROR: This wheel test script is only for Linux platforms." >/dev/stderr
  exit 1
fi

PYTHON_VERSION="${PYTHON//./}"

which python

which pip

RAY_PLACEHOLDER_VERSION="100.0.0-dev"
MINIFORGE_BIN_PATH="/opt/miniforge/bin"
PYTHON_EXE="${MINIFORGE_BIN_PATH}/python"
PIP_CMD="${MINIFORGE_BIN_PATH}/pip"
PIP_COMPILE_CMD="${MINIFORGE_BIN_PATH}/pip-compile"
# Find the appropriate wheel by grepping for the Python version.
PYTHON_WHEEL=$(find ./.whl -maxdepth 1 -type f -name "*${PYTHON_VERSION}*.whl" -print -quit)

if [[ -z "$PYTHON_WHEEL" ]]; then
  echo "No wheel found for pattern *${PYTHON_VERSION}*.whl" >/dev/stderr
  exit 1
fi

"$PYTHON_EXE" --version

"$PIP_CMD" install --upgrade pip

"$PIP_CMD" install pip-tools

"$PIP_COMPILE_CMD" --version

echo "ray[all]==${RAY_PLACEHOLDER_VERSION}" > ray-requirement.txt

"$PIP_COMPILE_CMD" ray-requirement.txt -o /ray.lock --find-links=.whl/

echo "✅ Completed ray placeholder wheel test"

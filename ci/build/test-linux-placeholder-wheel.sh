#!/bin/bash
set -exuo pipefail

PYTHON="$1"

if [[ ! "${OSTYPE}" =~ ^linux ]]; then
  echo "ERROR: This wheel test script is only for Linux platforms." >/dev/stderr
  exit 1
fi

which python

which pip

RAY_PLACEHOLDER_VERSION="100.0.0-dev"
PYTHON_EXE="python"
PIP_CMD="pip"
PIP_COMPILE_CMD="pip-compile"
# PYTHON_EXE="/opt/python/${PYTHON}/bin/python"
# PIP_CMD="$(dirname "$PYTHON_EXE")/pip"
# PIP_COMPILE_CMD="$(dirname "$PYTHON_EXE")/pip-compile"
# Find the appropriate wheel by grepping for the Python version.
PYTHON_WHEEL=$(find ./.whl -maxdepth 1 -type f -name "*${PYTHON}*.whl" -print -quit)

if [[ -z "$PYTHON_WHEEL" ]]; then
  echo "No wheel found for pattern *${PYTHON}*.whl" >/dev/stderr
  exit 1
fi

"$PYTHON_EXE" --version

"$PIP_CMD" install --upgrade pip

"$PIP_CMD" install pip-tools

"$PIP_COMPILE_CMD" --version

echo "ray[all]==${RAY_PLACEHOLDER_VERSION}" > ray-requirement.txt

"$PIP_COMPILE_CMD" ray-requirement.txt -o /ray.lock --find-links=.whl/

echo "✅ Completed ray placeholder wheel test"

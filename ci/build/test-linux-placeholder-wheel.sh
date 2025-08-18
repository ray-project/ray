#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

PYTHON="$1"
echo "PYTHON: $PYTHON"

ROOT_DIR=$(cd "$(dirname "$0")/$(dirname "$(test -L "$0" && readlink "$0" || echo "/")")"; pwd)

echo "ROOT_DIR: $ROOT_DIR"

if [[ ! "${OSTYPE}" =~ ^linux ]]; then
  echo "ERROR: This wheel test script is only for Linux platforms." >/dev/stderr
  exit 1
fi

# TODO (elliot-barn): list python versions
ls -d -- /opt/python/*/bin/

PYTHON_EXE="/opt/python/${PYTHON}/bin/python"
PIP_CMD="$(dirname "$PYTHON_EXE")/pip"

# Find the appropriate wheel by grepping for the Python version.
PYTHON_WHEEL=$(find ./.whl -maxdepth 1 -type f -name "*${PYTHON}*.whl" -print -quit)
echo "PYTHON_WHEEL: $PYTHON_WHEEL"

if [[ -z "$PYTHON_WHEEL" ]]; then
  echo "No wheel found for pattern *${PYTHON}*.whl" >/dev/stderr
  exit 1
fi

# Print some env info
"$PYTHON_EXE" --version

# Update pip
"$PIP_CMD" install --upgrade pip

# Install the wheel.
"$PIP_CMD" uninstall -y ray
"$PIP_CMD" install -q --no-deps "$PYTHON_WHEEL" --use-pep517

# TODO (elliot-barn): Test the wheel content (should be only METADATA)

#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

which python
ROOT_DIR=$(cd "$(dirname "$0")/$(dirname "$(test -L "$0" && readlink "$0" || echo "/")")"; pwd)

echo "ROOT_DIR: $ROOT_DIR"

if [[ ! "${OSTYPE}" =~ ^linux ]]; then
  echo "ERROR: This wheel test script is only for Linux platforms." >/dev/stderr
  exit 1
fi

PY_WHEEL_VERSIONS=("39")
PY_MMS=("3.9")

for ((i=0; i<${#PY_MMS[@]}; ++i)); do
  PY_MM="${PY_MMS[i]}"

  PY_WHEEL_VERSION="${PY_WHEEL_VERSIONS[i]}"

  PYTHON_EXE="opt/python/$PY_MM/bin/python$PY_MM"
  PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"

  # Find the appropriate wheel by grepping for the Python version.
  PYTHON_WHEEL="$(printf "%s\n" "$ROOT_DIR"/../../.whl/*"cp$PY_WHEEL_VERSION-cp$PY_WHEEL_VERSION"* | head -n 1)"
  echo "PYTHON_WHEEL: $PYTHON_WHEEL"

  # Print some env info
  "$PYTHON_EXE" --version
  "$PYTHON_EXE" -c "from distutils import util; print(util.get_platform())" || true

  # Update pip
  "$PIP_CMD" install -U pip

  # Install the wheel.
  "$PIP_CMD" uninstall -y ray
  "$PIP_CMD" install -q --no-deps "$PYTHON_WHEEL"

  # TODO (elliot-barn): Test the wheel content (should be only METADATA)

done

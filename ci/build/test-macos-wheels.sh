#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "$0")/$(dirname "$(test -L "$0" && readlink "$0" || echo "/")")"; pwd)

if [[ ! "${OSTYPE}" =~ ^darwin ]]; then
  echo "ERROR: This wheel test script is only for MacOS platforms." >/dev/stderr
  exit 1
fi

BUILD_DIR="${TRAVIS_BUILD_DIR-}"
if [[ -z "${BUILD_DIR}" ]]; then
  BUILD_DIR="${GITHUB_WORKSPACE}"
fi
if [[ -n "${BUILDKITE-}" ]]; then
  BUILD_DIR="${ROOT_DIR}/../.."
fi
TEST_DIR="${BUILD_DIR}/python/ray/tests"
TEST_SCRIPTS=("$TEST_DIR/test_microbenchmarks.py" "$TEST_DIR/test_basic.py")


function retry {
  local n=1
  local max=3

  while true; do
    if "$@"; then
      break
    fi
    if [ $n -lt $max ]; then
      ((n++))
      echo "Command failed. Attempt $n/$max:"
    else
      echo "The command has failed after $n attempts."
      exit 1
    fi
  done
}

MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions

PY_WHEEL_VERSIONS=("39" "310")
PY_MMS=("3.9" "3.10")

for ((i=0; i<${#PY_MMS[@]}; ++i)); do
  PY_MM="${PY_MMS[i]}"

  PY_WHEEL_VERSION="${PY_WHEEL_VERSIONS[i]}"

  # Todo: The main difference between arm64 and x86_64 is
  # the Mac OS version. We should move everything to a
  # single path when it's acceptable to move up our lower
  # Python + MacOS compatibility bound.
  if [[ "$(uname -m)" == "arm64" ]]; then
    CONDA_ENV_NAME="test-wheels-p$PY_MM"

    [[ -f "$HOME/.bash_profile" ]] && conda init bash

    source ~/.bash_profile

    conda create -y -n "$CONDA_ENV_NAME"
    conda activate "$CONDA_ENV_NAME"
    conda remove -y python || true
    conda install -y python="${PY_MM}"

    PYTHON_EXE="/opt/homebrew/opt/miniforge/envs/${CONDA_ENV_NAME}/bin/python"
    PIP_CMD="/opt/homebrew/opt/miniforge/envs/${CONDA_ENV_NAME}/bin/pip"
  else
    PYTHON_EXE="$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM"
    PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"
  fi

  # Find the appropriate wheel by grepping for the Python version.
  PYTHON_WHEEL="$(printf "%s\n" "$ROOT_DIR"/../../.whl/*"cp$PY_WHEEL_VERSION-cp$PY_WHEEL_VERSION"* | head -n 1)"

  # Print some env info
  "$PYTHON_EXE" --version
  "$PYTHON_EXE" -c "from distutils import util; print(util.get_platform())" || true

  # Update pip
  "$PIP_CMD" install -U pip

  # Install the wheel.
  "$PIP_CMD" uninstall -y ray
  "$PIP_CMD" install -q "$PYTHON_WHEEL"

  # Install the dependencies to run the tests.
  "$PIP_CMD" install -q aiohttp numpy 'pytest==7.4.4' requests proxy.py

  # Run a simple test script to make sure that the wheel works.
  # We set the python path to prefer the directory of the wheel content: https://github.com/ray-project/ray/pull/30090
  for SCRIPT in "${TEST_SCRIPTS[@]}"; do
    PY_IGNORE_IMPORTMISMATCH=1 PATH="$(dirname "$PYTHON_EXE"):$PATH" retry "$PYTHON_EXE" "$SCRIPT"
  done
done

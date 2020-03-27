#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

platform="unknown"
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  echo "Platform is linux."
  platform="linux"
elif [[ "$unamestr" == "Darwin" ]]; then
  echo "Platform is macosx."
  platform="macosx"
else
  echo "Unrecognized platform."
  exit 1
fi

TEST_DIR="$TRAVIS_BUILD_DIR/python/ray/tests"
TEST_SCRIPTS=("$TEST_DIR/test_microbenchmarks.py" "$TEST_DIR/test_basic.py")
UI_TEST_SCRIPT="$TRAVIS_BUILD_DIR/python/ray/tests/test_webui.py"

if [[ "$platform" == "linux" ]]; then
  # Install miniconda.
  PY_MMS=("3.6"
          "3.7"
          "3.8")
  MINICONDA_URLS=("https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh"
                  "https://repo.continuum.io/miniconda/Miniconda3-py37_4.8.2-Linux-x86_64.sh"
                  "https://repo.continuum.io/miniconda/Miniconda3-py38_4.8.2-Linux-x86_64.sh")

  for ((i=0; i<${#PY_MMS[@]}; ++i)); do
    PY_MM="${PY_MMS[i]}"
    PY_WHEEL_VERSION="${PY_MM//./}"
    MINICONDA_URL="${MINICONDA_URLS[i]}"
    rm -r -f -- "$HOME/miniconda3"
    wget --quiet "${MINICONDA_URL}" -O miniconda3.sh
    bash miniconda3.sh -b -p "$HOME/miniconda3"

    PYTHON_EXE="$HOME/miniconda3/bin/python"
    PIP_CMD="$HOME/miniconda3/bin/pip"

    # Find the right wheel by grepping for the Python version.
    PYTHON_WHEEL=$(find "$ROOT_DIR/../../.whl" -type f -maxdepth 1 -print -name "*${PY_WHEEL_VERSION}*")

    # Install the wheel.
    "$PIP_CMD" install -q "$PYTHON_WHEEL"

    # Check that ray.__commit__ was set properly.
    "$PYTHON_EXE" -u -c "import ray; print(ray.__commit__)" | grep "$TRAVIS_COMMIT" || (echo "ray.__commit__ not set properly!" && exit 1)

    # Run a simple test script to make sure that the wheel works.
    INSTALLED_RAY_DIRECTORY=$(dirname "$($PYTHON_EXE -u -c "import ray; print(ray.__file__)" | tail -n1)")

    for SCRIPT in "${TEST_SCRIPTS[@]}"; do
        "$PYTHON_EXE" "$SCRIPT"
    done

    # Run the UI test to make sure that the packaged UI works.
    "$PIP_CMD" install -q aiohttp google grpcio psutil requests setproctitle
    "$PYTHON_EXE" "$UI_TEST_SCRIPT"
  done

  # Check that the other wheels are present.
  NUMBER_OF_WHEELS=$(ls -1q "$ROOT_DIR"/../../.whl/*.whl | wc -l)
  if [[ "$NUMBER_OF_WHEELS" != "4" ]]; then
    echo "Wrong number of wheels found."
    ls -l "$ROOT_DIR/../.whl/"
    exit 2
  fi

elif [[ "$platform" == "macosx" ]]; then
  MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
  PY_MMS=("3.5"
          "3.6"
          "3.7"
          "3.8")

  for ((i=0; i<${#PY_MMS[@]}; ++i)); do
    PY_MM="${PY_MMS[i]}"
    PY_WHEEL_VERSION="${PY_MM//./}"

    PYTHON_EXE="$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM"
    PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"

    # Find the appropriate wheel by grepping for the Python version.
    PYTHON_WHEEL=$(find "$ROOT_DIR/../../.whl" -type f -maxdepth 1 -print -name "*${PY_WHEEL_VERSION}*")

    # Install the wheel.
    "$PIP_CMD" install -q "$PYTHON_WHEEL"

    # Run a simple test script to make sure that the wheel works.
    INSTALLED_RAY_DIRECTORY=$(dirname "$($PYTHON_EXE -u -c "import ray; print(ray.__file__)" | tail -n1)")
    for SCRIPT in "${TEST_SCRIPTS[@]}"; do
      "$PYTHON_EXE" "$SCRIPT"
    done

    if (( $(echo "$PY_MM >= 3.0" | bc) )); then
      # Run the UI test to make sure that the packaged UI works.
      "$PIP_CMD" install -q aiohttp google grpcio psutil requests setproctitle
      "$PYTHON_EXE" "$UI_TEST_SCRIPT"
    fi

  done
else
  echo "Unrecognized environment."
  exit 3
fi

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

TEST_SCRIPT="tests/test_microbenchmarks.py"
UI_TEST_SCRIPT="tests/test_webui.py"

if [[ "$platform" == "linux" ]]; then
  # First test Python 2.7.

  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda2-4.5.4-Linux-x86_64.sh -O miniconda2.sh
  bash miniconda2.sh -b -p "$HOME/miniconda2"

  PYTHON_EXE=$HOME/miniconda2/bin/python
  PIP_CMD=$HOME/miniconda2/bin/pip

  # Find the right wheel by grepping for the Python version.
  PYTHON_WHEEL=$(find "$ROOT_DIR/../../.whl" -type f -maxdepth 1 -print | grep -m1 '27')

  # Install the wheel.
  $PIP_CMD install -q "$PYTHON_WHEEL"

  # Run a simple test script to make sure that the wheel works.
  INSTALLED_RAY_DIRECTORY=$(dirname "$($PYTHON_EXE -u -c "import ray; print(ray.__file__)" | tail -n1)")
  $PYTHON_EXE -m pytest -v "$INSTALLED_RAY_DIRECTORY/$TEST_SCRIPT"

  # Now test Python 3.6.

  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda3.sh
  bash miniconda3.sh -b -p "$HOME/miniconda3"

  PYTHON_EXE=$HOME/miniconda3/bin/python
  PIP_CMD=$HOME/miniconda3/bin/pip

  # Find the right wheel by grepping for the Python version.
  PYTHON_WHEEL=$(find "$ROOT_DIR/../../.whl" -type f -maxdepth 1 -print | grep -m1 '36')

  # Install the wheel.
  $PIP_CMD install -q "$PYTHON_WHEEL"

  # Run a simple test script to make sure that the wheel works.
  INSTALLED_RAY_DIRECTORY=$(dirname "$($PYTHON_EXE -u -c "import ray; print(ray.__file__)" | tail -n1)")
  $PYTHON_EXE -m pytest -v "$INSTALLED_RAY_DIRECTORY/$TEST_SCRIPT"

  # Run the UI test to make sure that the packaged UI works.
  $PIP_CMD install -q aiohttp psutil requests setproctitle
  $PYTHON_EXE -m pytest -v "$INSTALLED_RAY_DIRECTORY/$UI_TEST_SCRIPT"

  # Check that the other wheels are present.
  NUMBER_OF_WHEELS=$(ls -1q "$ROOT_DIR"/../../.whl/*.whl | wc -l)
  if [[ "$NUMBER_OF_WHEELS" != "4" ]]; then
    echo "Wrong number of wheels found."
    ls -l "$ROOT_DIR/../.whl/"
    exit 2
  fi

elif [[ "$platform" == "macosx" ]]; then
  MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
  PY_MMS=("2.7"
          "3.5"
          "3.6"
          "3.7")
  # This array is just used to find the right wheel.
  PY_WHEEL_VERSIONS=("27"
                     "35"
                     "36"
                     "37")

  for ((i=0; i<${#PY_MMS[@]}; ++i)); do
    PY_MM=${PY_MMS[i]}
    PY_WHEEL_VERSION=${PY_WHEEL_VERSIONS[i]}

    PYTHON_EXE=$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM
    PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"

    # Find the appropriate wheel by grepping for the Python version.
    PYTHON_WHEEL=$(find "$ROOT_DIR/../../.whl" -type f -maxdepth 1 -print | grep -m1 "$PY_WHEEL_VERSION")

    # Install the wheel.
    $PIP_CMD install -q "$PYTHON_WHEEL"

    # Run a simple test script to make sure that the wheel works.
    INSTALLED_RAY_DIRECTORY=$(dirname "$($PYTHON_EXE -u -c "import ray; print(ray.__file__)" | tail -n1)")
    $PYTHON_EXE -m pytest -v "$INSTALLED_RAY_DIRECTORY/$TEST_SCRIPT"

    if (( $(echo "$PY_MM >= 3.0" | bc) )); then
      # Run the UI test to make sure that the packaged UI works.
      $PIP_CMD install -q aiohttp psutil requests setproctitle
      $PYTHON_EXE -m pytest -v "$INSTALLED_RAY_DIRECTORY/$UI_TEST_SCRIPT"
    fi
  done
else
  echo "Unrecognized environment."
  exit 3
fi

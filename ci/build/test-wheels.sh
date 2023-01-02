#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "$0")/$(dirname "$(test -L "$0" && readlink "$0" || echo "/")")"; pwd)

platform=""
case "${OSTYPE}" in
  linux*) platform="linux";;
  darwin*) platform="macosx";;
  msys*) platform="windows";;
  *) echo "Unrecognized platform."; exit 1;;
esac

BUILD_DIR="${TRAVIS_BUILD_DIR-}"
if [ -z "${BUILD_DIR}" ]; then
  BUILD_DIR="${GITHUB_WORKSPACE}"
fi
if [ -n "${BUILDKITE-}" ]; then
  BUILD_DIR="${ROOT_DIR}/../.."
fi
TEST_DIR="${BUILD_DIR}/python/ray/tests"
TEST_SCRIPTS=("$TEST_DIR/test_microbenchmarks.py" "$TEST_DIR/test_basic.py")
DASHBOARD_TEST_SCRIPT="${BUILD_DIR}/python/ray/tests/test_dashboard.py"


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

if [[ "$platform" == "linux" ]]; then
  # Install miniconda.
  PY_WHEEL_VERSIONS=("36" "37" "38" "39")
  PY_MMS=("3.6.13"
          "3.7.10"
          "3.8.10"
          "3.9.5")
  wget --quiet "https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh" -O miniconda3.sh
  "${ROOT_DIR}"/../suppress_output bash miniconda3.sh -b -p "$HOME/miniconda3"
  export PATH="$HOME/miniconda3/bin:$PATH"

  for ((i=0; i<${#PY_MMS[@]}; ++i)); do
    PY_MM="${PY_MMS[i]}"
    PY_WHEEL_VERSION="${PY_WHEEL_VERSIONS[i]}"

    conda install -y python="${PY_MM}"

    PYTHON_EXE="$HOME/miniconda3/bin/python"
    PIP_CMD="$HOME/miniconda3/bin/pip"

    # Find the right wheel by grepping for the Python version.
    PYTHON_WHEEL="$(printf "%s\n" "$ROOT_DIR"/../../.whl/*"$PY_WHEEL_VERSION"* | head -n 1)"

    # Install the wheel.
    "$PIP_CMD" install -q "$PYTHON_WHEEL"

    # Check that ray.__commit__ was set properly.
    "$PYTHON_EXE" -u -c "import ray; print(ray.__commit__)" | grep "$TRAVIS_COMMIT" || (echo "ray.__commit__ not set properly!" && exit 1)

    # Install the dependencies to run the tests.
    "$PIP_CMD" install -q aiohttp aiosignal frozenlist grpcio 'pytest==7.0.1' requests proxy.py

    # Run a simple test script to make sure that the wheel works.
    for SCRIPT in "${TEST_SCRIPTS[@]}"; do
        retry "$PYTHON_EXE" "$SCRIPT"
    done
    retry "$PYTHON_EXE" "$DASHBOARD_TEST_SCRIPT"
  done

  # Check that the other wheels are present.
  NUMBER_OF_WHEELS="$(find "$ROOT_DIR"/../../.whl/ -mindepth 1 -maxdepth 1 -name "*.whl" | wc -l)"
  if [[ "$NUMBER_OF_WHEELS" != "5" ]]; then
    echo "Wrong number of wheels found."
    ls -l "$ROOT_DIR/../.whl/"
    exit 2
  fi

elif [[ "$platform" == "macosx" ]]; then
  MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
  PY_WHEEL_VERSIONS=("36" "37" "38" "39" "310" "311")
  PY_MMS=("3.6"
          "3.7"
          "3.8"
          "3.9"
          "3.10"
          "3.11")

  for ((i=0; i<${#PY_MMS[@]}; ++i)); do
    PY_MM="${PY_MMS[i]}"

    PY_WHEEL_VERSION="${PY_WHEEL_VERSIONS[i]}"

    PYTHON_EXE="$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM"
    PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"

    # Find the appropriate wheel by grepping for the Python version.
    PYTHON_WHEEL="$(printf "%s\n" "$ROOT_DIR"/../../.whl/*"$PY_WHEEL_VERSION"* | head -n 1)"

    # Install the wheel.
    "$PIP_CMD" uninstall -y ray
    "$PIP_CMD" install -q "$PYTHON_WHEEL"

    # Install the dependencies to run the tests.
    "$PIP_CMD" install -q aiohttp aiosignal frozenlist grpcio 'pytest==7.0.1' requests proxy.py

    # Run a simple test script to make sure that the wheel works.
    # We set the python path to prefer the directory of the wheel content: https://github.com/ray-project/ray/pull/30090
    for SCRIPT in "${TEST_SCRIPTS[@]}"; do
      PY_IGNORE_IMPORTMISMATCH=1 PATH="$(dirname "$PYTHON_EXE"):$PATH" retry "$PYTHON_EXE" "$SCRIPT"
    done
  done
elif [ "${platform}" = windows ]; then
  echo "WARNING: Wheel testing not yet implemented for Windows."
else
  echo "Unrecognized environment."
  exit 3
fi

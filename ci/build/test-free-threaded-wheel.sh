#!/bin/bash
# Verifies the experimental free-threaded Ray wheel in clean environments, inside a
# manylinux image that provides the python3.15 interpreters.
#
# Usage: test-free-threaded-wheel.sh <cp315t-wheel-dir> <cp315-wheel-dir>
#   cp315t wheel: the worker side, installed with Ray's core requirements only.
#   cp315 wheel:  the regular CPython driver and cluster for the mixed-cluster test.
#
# Expects the repo's python/ray/tests, pytest.ini and ci/build/ at the same relative
# paths as in the repo, next to the wheel directories.

set -euxo pipefail

FT_WHEEL_DIR="$1"
REGULAR_WHEEL_DIR="$2"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

FT_TAG="cp315-cp315t"
REGULAR_TAG="cp315-cp315"
for tag in "$FT_TAG" "$REGULAR_TAG"; do
  [[ -d "/opt/python/${tag}" ]] || sudo manylinux-interpreters ensure "$tag"
done

FT_WHEEL="$(ls "$FT_WHEEL_DIR"/*.whl)"
REGULAR_WHEEL="$(ls "$REGULAR_WHEEL_DIR"/*.whl)"
PYTEST_PACKAGES=(pytest pytest-timeout pytest-asyncio)

# Worker environment: the wheel and its core requirements, nothing else.
"/opt/python/${FT_TAG}/bin/python" -m venv /tmp/ft-worker-env
/tmp/ft-worker-env/bin/pip install "$FT_WHEEL"

# Informational: reports which module, if any, re-enables the GIL on import.
/tmp/ft-worker-env/bin/python "$ROOT/ci/build/free_threaded_import_smoke.py"

# All-free-threaded: driver, cluster and worker on python3.15t.
"/opt/python/${FT_TAG}/bin/python" -m venv /tmp/ft-driver-env
/tmp/ft-driver-env/bin/pip install "$FT_WHEEL" "${PYTEST_PACKAGES[@]}"

# Mixed: regular CPython driver and cluster, free-threaded workers via py_executable.
"/opt/python/${REGULAR_TAG}/bin/python" -m venv /tmp/regular-env
/tmp/regular-env/bin/pip install "${REGULAR_WHEEL}[default]" "${PYTEST_PACKAGES[@]}"

# Tests import fixtures from the tests directory and must run from there.
TESTS_DIR=/tmp/ray-tests-run
mkdir "$TESTS_DIR"
cp -r "$ROOT/python/ray/tests/." "$TESTS_DIR/"
cp "$ROOT/pytest.ini" "$TESTS_DIR/"
cd "$TESTS_DIR"

# Both tests skip themselves when the interpreter setup is wrong, so a skip is a failure.
run_pytest() {
  local log
  log="$(mktemp)"
  "$@" 2>&1 | tee "$log"
  if grep -Eq '[0-9]+ skipped' "$log"; then
    echo "ERROR: free-threaded tests were skipped" >&2
    return 1
  fi
}

PYTHON_GIL=0 run_pytest /tmp/ft-driver-env/bin/python -m pytest -v -p no:cacheprovider \
  test_free_threaded_worker_threads.py

RAY_TEST_FREE_THREADED_PYTHON=/tmp/ft-worker-env/bin/python \
  run_pytest /tmp/regular-env/bin/python -m pytest -v -p no:cacheprovider \
  test_free_threaded_py_executable.py

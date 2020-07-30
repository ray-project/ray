#!/usr/bin/env bash

set -euxo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"

asan_install() {
  (cd "${ROOT_DIR}"/../python && pip install -e . --verbose)
}

asan_setup() {
  echo "Setting up the environment"
  pip install -r ray-project/requirements.txt
  pip install -U pytest==5.4.3

  echo "Installing cython example"
  (cd "${ROOT_DIR}"/../doc/examples/cython && python setup.py install --user)

  echo "Settting up the shell"
  echo "build --config=asan" >> ~/.bazelrc  # Setup cache
  echo "LD_PRELOAD=/usr/lib/gcc/x86_64-linux-gnu/7/libasan.so" >> ~/.bashrc
  echo "ASAN_OPTIONS=detect_leaks=0" >> ~/.bashrc

  echo "Compiling ray"
  git fetch
  git pull origin master
  asan_install || true
}

asan_run() {
  (
    export LD_PRELOAD="/usr/lib/gcc/x86_64-linux-gnu/7/libasan.so"
    export ASAN_OPTIONS="detect_leaks=0"

    cd "${ROOT_DIR}"/../..

    # async plasma test
    python -m pytest -v --durations=5 --timeout=300 python/ray/tests/test_async.py

    # Ray tests
    bazel test --test_tag_filters=-jenkins_only python/ray/serve/...
    bazel test --test_tag_filters=-jenkins_only python/ray/dashboard/...
    bazel test --test_tag_filters=-jenkins_only python/ray/tests/...
    bazel test --test_tag_filters=-jenkins_only python/ray/tune/...
  )
}

asan_recompile() {
  git fetch
  git checkout "$1"
  asan_install || true
}

if [ 0 -lt "$#" ]; then
  "asan_$1" "${@:2}"
else
  echo "Available commands: setup, run, recompile"
fi

#!/usr/bin/env bash

set -euxo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"

asan_install() {
  echo "installing"
  (cd "${ROOT_DIR}"/../ray/python && pip install -e . --verbose)
}

asan_setup() {
  echo "Setting up the environment"
  pip uninstall -y ray || true
  conda uninstall -y wrapt || true
  pip install wrapt || true
  pip install -r ray-project/requirements.txt
  pip install -U pytest==5.4.3

  echo "Installing cython example"
  (cd "${ROOT_DIR}"/../ray/doc/examples/cython && python setup.py install --user)

  echo "Settting up the shell"
  echo "build --config=asan" >> ~/.bazelrc  # Setup cache
  echo "LD_PRELOAD=/usr/lib/gcc/x86_64-linux-gnu/7/libasan.so" >> ~/.bashrc
  echo "ASAN_OPTIONS=detect_leaks=0" >> ~/.bashrc

  echo "Compiling ray"
  cd "${ROOT_DIR}"/../ray/
  git fetch
  git pull origin master
  asan_install || true
}

asan_run() {
  (
    export LD_PRELOAD="/usr/lib/gcc/x86_64-linux-gnu/7/libasan.so"
    export ASAN_OPTIONS="detect_leaks=0"

    cd "${ROOT_DIR}"/../ray

    # Ray tests
    bazel test --test_tag_filters=-jenkins_only --test_output=streamed python/ray/serve/...
    bazel test --test_tag_filters=-jenkins_only --test_output=streamed python/ray/dashboard/...
    bazel test --test_tag_filters=-jenkins_only --test_output=streamed python/ray/tests/...
    bazel test --test_tag_filters=-jenkins_only --test_output=streamed python/ray/tune/...
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

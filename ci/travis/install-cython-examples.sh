#!/usr/bin/env bash

set -euxo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

install_cython_examples() {
  (
    cd "${ROOT_DIR}/../../doc/examples/cython"
    pip install scipy
    python setup.py install --user
  )
}

install_cython_examples "$@"

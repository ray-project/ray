#!/usr/bin/env bash

set -eo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

install_cython_examples() {
  (
    cd "${ROOT_DIR}/../../doc/examples/cython"
    pip install scipy
    python setup.py install --user
  )
  eval "$(curl -sL https://raw.githubusercontent.com/travis-ci/gimme/master/gimme | GIMME_GO_VERSION=master bash)"
}

install_cython_examples "$@"

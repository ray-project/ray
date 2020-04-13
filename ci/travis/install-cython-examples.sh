#!/usr/bin/env bash

{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null  # Push caller's shell options (quietly)

set -eo pipefail && if [ -n "${OSTYPE##darwin*}" ]; then set -u; fi  # some options interfere with Travis's RVM on Mac

ROOT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

install_cython_examples() {
  (
    builtin cd "${ROOT_DIR}/../../doc/examples/cython"
    pip install scipy
    python setup.py install --user
  )
  eval "$(curl -sL https://raw.githubusercontent.com/travis-ci/gimme/master/gimme | GIMME_GO_VERSION=master bash)"
}

install_cython_examples "$@"

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)

#!/usr/bin/env bash

{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null  # Push caller's shell options (quietly)

set -eo pipefail && if [ -n "${OSTYPE##darwin*}" ]; then set -u; fi  # some options interfere with Travis's RVM on Mac

ROOT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
WORKSPACE_DIR="${ROOT_DIR}/../.."

npm_run() {
  if [ "${OSTYPE}" = msys ]; then
    { echo "WARNING: Skipping running NPM due to package incompatibilities with Windows"; } 2> /dev/null
  else
    (
      builtin cd ray/dashboard/client
      set +x  # suppress set -x since it'll get very noisy here
      . "${HOME}/.nvm/nvm.sh"
      nvm use --silent node
      npm ci
      npm run -s build
    )
  fi
}

install_ray() {
  (
    builtin cd "${WORKSPACE_DIR}"/python
    npm_run
    if [ "${OSTYPE}" = msys ]; then
      "${WORKSPACE_DIR}"/ci/keep_alive pip install -v -e . || echo "WARNING: Ignoring Ray package build failure on Windows for now" 1>&2
    else
      "${WORKSPACE_DIR}"/ci/keep_alive pip install -v -e .
    fi
  )
}

install_ray "$@"

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)

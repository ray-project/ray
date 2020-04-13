#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
WORKSPACE_DIR="${ROOT_DIR}/../.."

npm_run() {
  if [ "${OSTYPE}" = msys ]; then
    { echo "WARNING: Skipping running NPM due to package incompatibilities with Windows"; } 2> /dev/null
  else
    (
      cd ray/dashboard/client
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
    cd "${WORKSPACE_DIR}"/python
    npm_run
    if [ "${OSTYPE}" = msys ]; then
      "${WORKSPACE_DIR}"/ci/keep_alive pip install -v -e . || echo "WARNING: Ignoring Ray package build failure on Windows for now" 1>&2
    else
      "${WORKSPACE_DIR}"/ci/keep_alive pip install -v -e .
    fi
  )
}

install_ray "$@"

#!/usr/bin/env bash

set -euxo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
WORKSPACE_DIR="${ROOT_DIR}/../.."

build_dashboard_front_end() {
  if [ "${OSTYPE}" = msys ]; then
    { echo "WARNING: Not building dashboard front-end due to NPM package incompatibilities with Windows"; } 2> /dev/null
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
    build_dashboard_front_end
    if [ "${OSTYPE}" = msys ]; then
      "${WORKSPACE_DIR}"/ci/keep_alive pip install -v -e . || echo "WARNING: Ignoring Ray package build failure on Windows for now" 1>&2
    else
      "${WORKSPACE_DIR}"/ci/keep_alive pip install -v -e .
    fi
  )
}

install_ray "$@"

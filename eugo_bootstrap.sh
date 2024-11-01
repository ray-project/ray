#!/usr/bin/env bash
set -euo pipefail
set +e

export LD_PRELOAD=""
mkdir -p '/tmp/eugo/install_package_input/default'

# MARK: - 1. NodeJS
dnf install nodejs20 -y

update-alternatives --install /usr/bin/node node /usr/bin/node-20 2
update-alternatives --install /usr/bin/npm npm /usr/bin/npm-20 2


# MARK: - 2. Older Cython (v0.x)
export EUGO_CYTHON_OLD_PATH="${EUGO_STAGING_PATH_BASE}/_cython_old"

eugo_install_package \
  -m="eugo/cython_old/meta.json" \
  -s="eugo/cython_old/setup" \
  -i="${EUGO_CYTHON_OLD_PATH}"

export PYTHONPATH="${EUGO_CYTHON_OLD_PATH}${PYTHONPATH:+:$PYTHONPATH}"
export PATH="${EUGO_CYTHON_OLD_PATH}/bin:${PATH}"

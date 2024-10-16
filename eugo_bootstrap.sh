#!/usr/bin/env bash
set -euo pipefail


# MARK: - 1. NodeJS
dnf install nodejs20

update-alternatives --install /usr/bin/node node /usr/bin/node-20 2
update-alternatives --install /usr/bin/npm npm /usr/bin/node-20 2


# MARK: - 2. Older Cython (v0.x)
export EUGO_CYTHON_OLD_PATH="${EUGO_STAGING_PATH_BASE}/_cython_old"

export PYTHONPATH="${EUGO_CYTHON_OLD_PATH}${PYTHONPATH:+:$PYTHONPATH}"
export PATH="${EUGO_CYTHON_OLD_PATH}/bin:${PATH}"

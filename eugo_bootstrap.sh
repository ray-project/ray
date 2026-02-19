#!/usr/bin/env bash
set -euo pipefail
set +e

export LD_PRELOAD=""
mkdir -p '/tmp/eugo/install_package_input/default'

# MARK: - 1. NodeJS
EUGO_NODEJS_VERSION="24"
dnf install "nodejs${EUGO_NODEJS_VERSION}" -y

update-alternatives --install "/usr/bin/node node /usr/bin/node-${EUGO_NODEJS_VERSION}" 101
update-alternatives --install "/usr/bin/npm npm /usr/bin/npm-${EUGO_NODEJS_VERSION}" 101

# NOTE: older cython removed as not needed any longer
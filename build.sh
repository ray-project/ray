#!/usr/bin/env bash

set -eux
1>&2 echo "warning: ${0##*/} is deprecated and will be removed; please use pip install!"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
# Java is now automatically built. To disable, set RAY_INSTALL_JAVA=0 before calling this script.
RAY_INSTALL_JAVA="${RAY_INSTALL_JAVA-1}" python -m pip install -e "${ROOT_DIR}/python/"

#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ 0 -lt "$#" ]; then
  echo "error: ${0##*/} no longer accepts command-line arguments" 1>&2
  false
fi

1>&2 echo "warning: ${0##*/} is deprecated and will be removed; please use pip install!"
python -m pip install -e "${ROOT_DIR}/python/"

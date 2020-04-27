#!/usr/bin/env bash

if [ "${OSTYPE}" = msys ]; then
  echo "WARNING: ${0##*/} is not recommended on MSYS2, as MSYS2 alters the build environment."
fi

if [ -z "${PYTHON3_BIN_PATH-}" ]; then
  PYTHON3_BIN_PATH="$(command -v python3 || command -v python || echo python)"
fi

BAZEL_SH="${SHELL}" exec \
  "${PYTHON3_BIN_PATH}" -c \
  "import runpy, sys; runpy.run_path(sys.argv.pop(), run_name='__api__')" \
  build "$@" "${0%/*}"/python/setup.py

#!/usr/bin/env bash

prep_build_env() {
  export PATH="${PATH}:${HOME}/bin"
  if [ "${OSTYPE}" = "msys" ]; then
    export USE_CLANG_CL=1
    export MSYS2_ARG_CONV_EXCL="*"  # Don't let MSYS2 attempt to auto-translate arguments that look like paths
    local latest_python_bin=""
    for latest_python_bin in /proc/registry/HKEY_LOCAL_MACHINE/Software/Python/PythonCore/*/InstallPath/@; do
      if [ -f "${latest_python_bin}" ]; then
        latest_python_bin="$(tr -d '\0' < "${latest_python_bin}")"
        latest_python_bin="${latest_python_bin}\\"
      else
        latest_python_bin=""
      fi
    done
    latest_python_bin="${latest_python_bin}python.exe"
    if [ -f "${latest_python_bin}" ]; then
      export PYTHON2_BIN_PATH="${latest_python_bin}" PYTHON3_BIN_PATH="${latest_python_bin}"
    fi
  fi
}

if [ 0 -lt "$#" ]; then
  "$@"
fi

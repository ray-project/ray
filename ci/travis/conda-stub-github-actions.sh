#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)"

if [ "${OSTYPE}" = msys ]; then
  sudo() {
    command "$@"
  }
fi

_conda_hook() {
  sudo ln -s -f -- "${GITHUB_WORKSPACE}"/ci/travis/conda-stub-github-actions.sh "${CONDA_PREFIX}"/conda
}

conda_init() {
  if [ -z "${CONDA_PREFIX-}" ]; then
    local script_subdir
    case "${OSTYPE}" in
      msys) script_subdir=Scripts;;
      *) script_subdir=bin;;
    esac
    export CONDA_PREFIX="/usr/local/miniconda"
    echo "export CONDA_PREFIX=\"${CONDA_PREFIX}\" PATH=\"${CONDA_PREFIX}:${CONDA_PREFIX}/${script_subdir}:\${PATH}\"" >> ~/.bashrc
    export PATH="${CONDA_PREFIX}:${CONDA_PREFIX}/${script_subdir}:${PATH}"
  fi
  if [ -z "${CONDA_PYTHON_EXE-}" ]; then
    export CONDA_PYTHON_EXE="${CONDA_PREFIX}/python"
    echo "export CONDA_PYTHON_EXE=\"${CONDA_PYTHON_EXE}\"" >> ~/.bashrc
  fi
  if [ "${OSTYPE}" = msys ]; then
    case " ${MSYS-} " in
      *" winsymlinks:native"*)
        ;;
      *)
        echo 'export MSYS="${MSYS-} winsymlinks:native"' > /etc/profile.d/msys-env.sh
        . /etc/profile.d/msys-env.sh
        ;;
    esac
    # Ensure we can create symbolic links before moving on
    touch tempfile
    ln -s -- tempfile symlink.tmp
    test -L symlink.tmp
    rm -f -- tempfile symlink.tmp
  fi

  local dir="${CONDA_PREFIX}"
  sudo mkdir -p -- "${dir}"
  sudo chown "${USER-${USERNAME}}" "${dir}"
  sudo rm -f -- "${dir}"/conda
  _conda_hook
}

conda_install() {
  local arg result=1
  for arg in "$@"; do
    case "${arg}" in
      python=*)
        local version="${arg#*=}"
        local candidate
        for candidate in "${RUNNER_TOOL_CACHE}/Python/${version}"{,.*}/x64; do
          if [ -x "${candidate}" ]; then
            sudo mkdir -p -- "${CONDA_PREFIX}"
            sudo rm -r -f -- "${CONDA_PREFIX}"
            sudo ln -s -f -- "${candidate}" "${CONDA_PREFIX}"
            _conda_hook
            case "$(python --version 2>&1)." in
              "Python ${version}".*)
                pip config -q --user set global.no-warn-script-location false  # false actually means true here
                pip install --upgrade wheel pip || true
                result=0  # we found a matching Python version that works
                break  # stop searching
                ;;
              *)
                command -V python
                python --version 2>&1
                break
                ;;
            esac
          fi
        done
        ;;
      *)
        echo "Unsupported conda install target \"${arg}\": $*" 1>&2
        ;;
    esac
  done
  return "${result}"
}

conda() {
  echo "Running fake conda $*" 1>&2
  case "$1" in
    shell.*)
      ;;
    *)
      conda_"$@"
      ;;
  esac
}

conda "$@"

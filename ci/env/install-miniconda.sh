#!/usr/bin/env bash

install_miniconda() {
  if [ "${OSTYPE}" = msys ]; then
    # Windows is on GitHub Actions, whose built-in Python installations we added direct support for.
    python --version
    return 0
  fi

  local conda="${CONDA_EXE-}"  # Try to get the activated conda executable
  if [ -z "${conda}" ]; then  # If no conda is found, try to find it in PATH
    conda="$(command -v conda || true)"
  fi

  if [ ! -x "${conda}" ] || [ "${MINIMAL_INSTALL-}" = 1 ]; then  # If no conda is found, install it
    local miniconda_dir  # Keep directories user-independent, to help with Bazel caching
    local miniconda_version="Miniconda3-py311_24.4.0-0"
    local miniconda_platform=""
    local exe_suffix=".sh"

    case "${OSTYPE}" in
      linux*)
        miniconda_dir="/opt/miniconda"
        miniconda_platform=Linux
        ;;
      darwin*)
        if [ "$(uname -m)" = "arm64" ]; then
          HOSTTYPE="arm64"
          miniconda_dir="/opt/homebrew/opt/miniconda"
        else
          HOSTTYPE="x86_64"
          miniconda_dir="/usr/local/opt/miniconda"
        fi
        miniconda_platform=MacOSX
        ;;
      msys*)
        miniconda_dir="${ALLUSERSPROFILE}\Miniconda3" # Avoid spaces; prefer the default path
        miniconda_platform=Windows
        exe_suffix=".exe"
        ;;
    esac

    local miniconda_url="https://repo.continuum.io/miniconda/${miniconda_version}-${miniconda_platform}-${HOSTTYPE}${exe_suffix}"
    local miniconda_target="${HOME}/${miniconda_url##*/}"
    curl -f -s -L -o "${miniconda_target}" "${miniconda_url}"
    chmod +x "${miniconda_target}"

    case "${OSTYPE}" in
      msys*)
        # We set /AddToPath=0 because
        # (1) it doesn't take care of the current shell, and
        # (2) it's consistent with -b in the UNIX installers.
        MSYS2_ARG_CONV_EXCL="*" "${miniconda_target}" \
          /RegisterPython=0 /AddToPath=0 /InstallationType=AllUsers /S /D="${miniconda_dir}"
        conda="${miniconda_dir}\Scripts\conda.exe"
        ;;
      *)
        if [ "${MINIMAL_INSTALL-}" = 1 ]; then
          rm -rf "${miniconda_dir}"
        fi
        mkdir -p -- "${miniconda_dir}"
        # We're forced to pass -b for non-interactive mode.
        # Unfortunately it inhibits PATH modifications as a side effect.
        "${WORKSPACE_DIR}"/ci/suppress_output "${miniconda_target}" -f -b -p "${miniconda_dir}"
        conda="${miniconda_dir}/bin/conda"
        ;;
    esac
  fi

  if [ ! -x "${CONDA_PYTHON_EXE-}" ]; then  # If conda isn't activated, activate it
    local restore_shell_state=""
    if [ -o xtrace ]; then set +x && restore_shell_state="set -x"; fi  # Disable set -x (noisy here)

    # TODO(mehrdadn): conda activation is buggy on MSYS2; it adds C:/... to PATH,
    # which gets split on a colon. Is it necessary to work around this?
    eval "$("${conda}" shell."${SHELL##*/}" hook)"  # Activate conda
    conda init "${SHELL##*/}"  # Add to future shells

    ${restore_shell_state}  # Restore set -x
  fi

  local python_version
  python_version="$(python -s -c "import sys; print('%s.%s' % sys.version_info[:2])")"
  if [ -n "${PYTHON-}" ] && [ "${PYTHON}" != "${python_version}" ]; then  # Update Python version
    (
      set +x
      echo "Updating Anaconda Python ${python_version} to ${PYTHON}..."
      "${WORKSPACE_DIR}"/ci/suppress_output conda install -q -y python="${PYTHON}"
    )
  elif [ "${MINIMAL_INSTALL-}" = "1" ]; then  # Reset environment
    (
      set +x
      echo "Resetting Anaconda Python ${python_version}..."
      "${WORKSPACE_DIR}"/ci/suppress_output conda install -q -y --rev 0
    )
  fi

  if [[ "${PYTHON-}" != "3.12" ]]; then
    # Install mpi4py as a test dependency for Python <3.12; currently mpi4py is not 
    # available for Python 3.12
    "${WORKSPACE_DIR}"/ci/suppress_output conda install -c anaconda mpi4py -y
  fi

  command -V python
  test -x "${CONDA_PYTHON_EXE}"  # make sure conda is activated
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then 
  set -exuo pipefail

  SCRIPT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
  WORKSPACE_DIR="${SCRIPT_DIR}/../.."
  install_miniconda
fi


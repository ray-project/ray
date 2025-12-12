#!/usr/bin/env bash

install_miniforge() {
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
    local miniforge_dir  # Keep directories user-independent, to help with Bazel caching
    local miniforge_version="Miniforge3-25.3.0-1"
    local miniforge_platform=""
    local exe_suffix=".sh"

    case "${OSTYPE}" in
      linux*)
        miniforge_dir="/opt/miniforge"
        miniforge_platform=Linux
        ;;
      darwin*)
        if [ "$(uname -m)" = "arm64" ]; then
          HOSTTYPE="arm64"
          miniforge_dir="/opt/homebrew/opt/miniforge"
        else
          HOSTTYPE="x86_64"
          miniforge_dir="/usr/local/opt/miniforge"
        fi
        miniforge_platform=MacOSX
        ;;
      msys*)
        miniforge_dir="${ALLUSERSPROFILE}\Miniforge3" # Avoid spaces; prefer the default path
        miniforge_platform=Windows
        exe_suffix=".exe"
        ;;
    esac

    local miniforge_url="https://github.com/conda-forge/miniforge/releases/download/25.3.0-1/${miniforge_version}-${miniforge_platform}-${HOSTTYPE}${exe_suffix}"
    local miniforge_target="${HOME}/${miniforge_url##*/}"
    curl -f -s -L -o "${miniforge_target}" "${miniforge_url}"
    chmod +x "${miniforge_target}"

    case "${OSTYPE}" in
      msys*)
        # We set /AddToPath=0 because
        # (1) it doesn't take care of the current shell, and
        # (2) it's consistent with -b in the UNIX installers.
        MSYS2_ARG_CONV_EXCL="*" "${miniforge_target}" \
          /RegisterPython=0 /AddToPath=0 /InstallationType=AllUsers /S /D="${miniforge_dir}"
        conda="${miniforge_dir}\Scripts\conda.exe"
        ;;
      *)
        if [ "${MINIMAL_INSTALL-}" = 1 ]; then
          rm -rf "${miniforge_dir}"
        fi
        mkdir -p -- "${miniforge_dir}"
        # We're forced to pass -b for non-interactive mode.
        # Unfortunately it inhibits PATH modifications as a side effect.
        "${WORKSPACE_DIR}"/ci/suppress_output "${miniforge_target}" -f -b -p "${miniforge_dir}"
        conda="${miniforge_dir}/bin/conda"
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
    local conda_dir
    conda_dir="$(dirname "$(dirname "${conda}")")"
    (
      set +x
      echo "Updating Anaconda Python ${python_version} to ${PYTHON}..."
      if [ "${PYTHON}" = "3.14" ]; then
        # Python 3.14 can't be installed via conda install because it would remove conda itself.
        # Create a separate environment and merge it back.
        "${WORKSPACE_DIR}"/ci/suppress_output mamba create -q -y -n py314 python="${PYTHON}" libgcc-ng libffi
        "${WORKSPACE_DIR}"/ci/suppress_output conda clean -q -y --all
        rm -rf "${conda_dir}/envs/py314/conda-meta"
        rsync -a "${conda_dir}/envs/py314/" "${conda_dir}/"
        rm -rf "${conda_dir}/envs/py314"
        rm -f "${conda_dir}/bin/conda" "${conda_dir}/bin/mamba"
        # Since conda is removed, create symlinks in /usr/local/bin for python/pip
        # This ensures they're in PATH for all shells without needing .bashrc
        ln -sf "${conda_dir}/bin/python" /usr/local/bin/python
        ln -sf "${conda_dir}/bin/python3" /usr/local/bin/python3
        ln -sf "${conda_dir}/bin/pip" /usr/local/bin/pip
        ln -sf "${conda_dir}/bin/pip3" /usr/local/bin/pip3
      else
        "${WORKSPACE_DIR}"/ci/suppress_output conda install -q -y python="${PYTHON}"
      fi
    )
  elif [ "${MINIMAL_INSTALL-}" = "1" ]; then  # Reset environment
    (
      set +x
      echo "Resetting Anaconda Python ${python_version}..."
      "${WORKSPACE_DIR}"/ci/suppress_output conda install -q -y --rev 0
    )
  fi

  command -V python
  test -x "${CONDA_PYTHON_EXE}"  # make sure conda is activated
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  set -exuo pipefail

  SCRIPT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
  WORKSPACE_DIR="${SCRIPT_DIR}/../.."
  install_miniforge
fi

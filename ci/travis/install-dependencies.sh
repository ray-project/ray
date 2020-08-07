#!/usr/bin/env bash

# Push caller's shell options (quietly)
{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null

set -euxo pipefail

ROOT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
WORKSPACE_DIR="${ROOT_DIR}/../.."

pkg_install_helper() {
  case "${OSTYPE}" in
    darwin*)
      brew install "$@"
      ;;
    linux*)
      sudo apt-get install -qq -o=Dpkg::Use-Pty=0 "$@" | {
        grep --line-buffered -v "^\(Preparing to unpack\|Unpacking\|Processing triggers for\) "
      }
      ;;
    *) false;;
  esac
}

install_bazel() {
  "${ROOT_DIR}"/install-bazel.sh
  if [ -f /etc/profile.d/bazel.sh ]; then
    . /etc/profile.d/bazel.sh
  fi
}

install_base() {
  case "${OSTYPE}" in
    linux*)
      # Expired apt key error: https://github.com/bazelbuild/bazel/issues/11470#issuecomment-633205152
      curl -f -s -L -R https://bazel.build/bazel-release.pub.gpg | sudo apt-key add - || true
      sudo apt-get update -qq
      pkg_install_helper build-essential curl unzip libunwind-dev python3-pip python3-setuptools \
        tmux gdb
      if [ "${LINUX_WHEELS-}" = 1 ]; then
        pkg_install_helper docker
        if [ -n "${TRAVIS-}" ]; then
          sudo usermod -a -G docker travis
        fi
      fi
      if [ -n "${PYTHON-}" ]; then
        "${ROOT_DIR}/install-strace.sh" || true
      fi
      ;;
  esac
}

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

  if [ ! -x "${conda}" ]; then  # If no conda is found, install it
    local miniconda_dir  # Keep directories user-independent, to help with Bazel caching
    case "${OSTYPE}" in
      linux*) miniconda_dir="/opt/miniconda";;
      darwin*) miniconda_dir="/usr/local/opt/miniconda";;
      msys) miniconda_dir="${ALLUSERSPROFILE}\Miniconda3";;  # Avoid spaces; prefer the default path
    esac

    local miniconda_version="Miniconda3-py37_4.8.2" miniconda_platform="" exe_suffix=".sh"
    case "${OSTYPE}" in
      linux*) miniconda_platform=Linux;;
      darwin*) miniconda_platform=MacOSX;;
      msys*) miniconda_platform=Windows; exe_suffix=".exe";;
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
        mkdir -p -- "${miniconda_dir}"
        # We're forced to pass -b for non-interactive mode.
        # Unfortunately it inhibits PATH modifications as a side effect.
        "${WORKSPACE_DIR}"/ci/suppress_output "${miniconda_target}" -f -b -p "${miniconda_dir}"
        conda="${miniconda_dir}/bin/conda"
        ;;
    esac
  else
    case "${OSTYPE}" in
      darwin*)
        # When 'conda' is preinstalled on Mac (as on GitHub Actions), it uses this directory
        local miniconda_dir="/usr/local/miniconda"
        sudo mkdir -p -- "${miniconda_dir}"
        sudo chown -R "${USER}" "${miniconda_dir}"
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
  fi

  command -V python
  test -x "${CONDA_PYTHON_EXE}"  # make sure conda is activated
}

install_shellcheck() {
  local shellcheck_version="0.7.1"
  if [ "${shellcheck_version}" != "$(command -v shellcheck > /dev/null && shellcheck --version | sed -n "s/version: //p")" ]; then
    local osname=""
    case "${OSTYPE}" in
      linux*) osname="linux";;
      darwin*) osname="darwin";;
    esac
    local name="shellcheck-v${shellcheck_version}"
    if [ "${osname}" = linux ] || [ "${osname}" = darwin ]; then
      sudo mkdir -p /usr/local/bin || true
      curl -f -s -L "https://github.com/koalaman/shellcheck/releases/download/v${shellcheck_version}/${name}.${osname}.x86_64.tar.xz" | {
        sudo tar -C /usr/local/bin -x -v -J --strip-components=1 "${name}/shellcheck"
      }
    else
      mkdir -p /usr/local/bin
      curl -f -s -L -o "${name}.zip" "https://github.com/koalaman/shellcheck/releases/download/v${shellcheck_version}/${name}.zip"
      unzip "${name}.zip" "${name}.exe"
      mv -f "${name}.exe" "/usr/local/bin/shellcheck.exe"
    fi
    test "${shellcheck_version}" = "$(shellcheck --version | sed -n "s/version: //p")"
  fi
}

install_linters() {
  pip install -r "${WORKSPACE_DIR}"/python/requirements_linters.txt

  install_shellcheck
}

install_nvm() {
  local NVM_HOME="${HOME}/.nvm"
  if [ "${OSTYPE}" = msys ]; then
    local ver="1.1.7"
    if [ ! -f "${NVM_HOME}/nvm.sh" ]; then
      mkdir -p -- "${NVM_HOME}"
      export NVM_SYMLINK="${PROGRAMFILES}\nodejs"
      (
        cd "${NVM_HOME}"
        local target="./nvm-${ver}.zip"
        curl -f -s -L -o "${target}" \
          "https://github.com/coreybutler/nvm-windows/releases/download/${ver}/nvm-noinstall.zip"
        unzip -q -- "${target}"
        rm -f -- "${target}"
        printf "%s\r\n" "root: $(cygpath -w -- "${NVM_HOME}")" "path: ${NVM_SYMLINK}" > settings.txt
      )
      printf "%s\n" \
        "export NVM_HOME=\"$(cygpath -w -- "${NVM_HOME}")\"" \
        "nvm() { \"\${NVM_HOME}/nvm.exe\" \"\$@\"; }" \
        > "${NVM_HOME}/nvm.sh"
    fi
  else
    test -f "${NVM_HOME}/nvm.sh"  # double-check NVM is already available on other platforms
  fi
}

install_pip() {
  local python=python
  if command -v python3 > /dev/null; then
    python=python3
  fi

  if "${python}" -m pip --version || "${python}" -m ensurepip; then  # Configure pip if present
    "${python}" -m pip install --upgrade --quiet pip

    # If we're in a CI environment, do some configuration
    if [ "${CI-}" = true ]; then
      "${python}" -W ignore -m pip config -q --user set global.disable-pip-version-check True
      "${python}" -W ignore -m pip config -q --user set global.no-color True
      "${python}" -W ignore -m pip config -q --user set global.progress_bar off
      "${python}" -W ignore -m pip config -q --user set global.quiet True
    fi
  fi
}

install_node() {
  if [ "${OSTYPE}" = msys ]; then
    { echo "WARNING: Skipping running Node.js due to incompatibilities with Windows"; } 2> /dev/null
  else
    # Install the latest version of Node.js in order to build the dashboard.
    (
      set +x # suppress set -x since it'll get very noisy here
      . "${HOME}/.nvm/nvm.sh"
      nvm install node
      nvm use --silent node
      npm config set loglevel warn  # make NPM quieter
    )
  fi
}

install_toolchains() {
  "${ROOT_DIR}"/install-toolchains.sh
}

install_dependencies() {

  install_bazel

  install_base
  install_toolchains
  install_nvm
  install_pip

  if [ -n "${PYTHON-}" ] || [ "${LINT-}" = 1 ]; then
    install_miniconda
  fi

  # Install modules needed in all jobs.
  pip install --no-clean dm-tree  # --no-clean is due to: https://github.com/deepmind/tree/issues/5

  if [ -n "${PYTHON-}" ]; then
    # PyTorch is installed first since we are using a "-f" directive to find the wheels.
    # We want to install the CPU version only.
    local torch_url="https://download.pytorch.org/whl/torch_stable.html"
    case "${OSTYPE}" in
      darwin*) pip install torch torchvision;;
      *) pip install torch==1.5.0+cpu torchvision==0.6.0+cpu -f "${torch_url}";;
    esac

    # Try n times; we often encounter OpenSSL.SSL.WantReadError (or others)
    # that break the entire CI job: Simply retry installation in this case
    # after n seconds.
    local status="0";
    local errmsg="";
    for _ in {1..3}; do
      errmsg=$(CC=gcc pip install -r "${WORKSPACE_DIR}"/python/requirements.txt 2>&1) && break;
      status=$errmsg && echo "'pip install ...' failed, will retry after n seconds!" && sleep 30;
    done
    if [ "$status" != "0" ]; then
      echo "${status}" && return 1
    fi
  fi

  if [ "${LINT-}" = 1 ]; then
    install_linters
    # readthedocs has an antiquated build env.
    # This is a best effort to reproduce it locally to avoid doc build failures and hidden errors.
    local python_version
    python_version="$(python -s -c "import sys; print('%s.%s' % sys.version_info[:2])")"
    if [ "${OSTYPE}" = msys ] && [ "${python_version}" = "3.8" ]; then
      { echo "WARNING: Pillow binaries not available on Windows; cannot build docs"; } 2> /dev/null
    else
      pip install -r "${WORKSPACE_DIR}"/doc/requirements-rtd.txt
      pip install -r "${WORKSPACE_DIR}"/doc/requirements-doc.txt
    fi
  fi

  # Additional RLlib dependencies.
  if [ "${RLLIB_TESTING-}" = 1 ]; then
    pip install -r "${WORKSPACE_DIR}"/python/requirements_rllib.txt
  fi

  # Additional Tune test dependencies.
  if [ "${TUNE_TESTING-}" = 1 ]; then
    pip install -r "${WORKSPACE_DIR}"/python/requirements_tune.txt
  fi

  # Additional RaySGD test dependencies.
  if [ "${SGD_TESTING-}" = 1 ]; then
    pip install -r "${WORKSPACE_DIR}"/python/requirements_tune.txt
  fi

  # Additional Doc test dependencies.
  if [ "${DOC_TESTING-}" = 1 ]; then
    pip install -r "${WORKSPACE_DIR}"/python/requirements_tune.txt
  fi

  # If CI has deemed that a different version of Tensorflow or Torch
  # should be installed, then upgrade/downgrade to that specific version.
  if [ -n "${TORCH_VERSION-}" ] || [ -n "${TFP_VERSION-}" ] || [ -n "${TF_VERSION-}" ]; then
    case "${TORCH_VERSION-1.6}" in
      1.5) TORCHVISION_VERSION=0.6.0;;
      *) TORCHVISION_VERSION=0.5.0;;
    esac

    pip install --upgrade tensorflow-probability=="${TFP_VERSION-0.8}" \
      torch=="${TORCH_VERSION-1.6}" torchvision=="${TORCHVISION_VERSION}" \
      tensorflow=="${TF_VERSION-2.2.0}" gym
  fi

  if [ -n "${PYTHON-}" ] || [ -n "${LINT-}" ] || [ "${MAC_WHEELS-}" = 1 ]; then
    install_node
  fi

  CC=gcc pip install psutil setproctitle --target="${WORKSPACE_DIR}/python/ray/thirdparty_files"
}

install_dependencies "$@"

# Pop caller's shell options (quietly)
{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null

#!/usr/bin/env bash

{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null  # Push caller's shell options (quietly)

set -euxo pipefail

ROOT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
WORKSPACE_DIR="${ROOT_DIR}/../.."

pkg_install_helper() {
  case "${OSTYPE}" in
    darwin*) brew install "$@";;
    linux*) sudo apt-get install -qq -o=Dpkg::Use-Pty=0 "$@" | grep --line-buffered -v "^\(Preparing to unpack\|Unpacking\|Processing triggers for\) ";;
    *) false;;
  esac
}

install_base() {
  case "${OSTYPE}" in
    linux*)
      sudo apt-get update -qq
      pkg_install_helper build-essential curl unzip tmux gdb libunwind-dev python3-pip python3-setuptools
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
  local miniconda_dir="${HOME}/miniconda"
  if [ "${OSTYPE}" = msys ]; then
    miniconda_dir="${miniconda_dir}/bin"  # HACK: Compensate for python.exe being in the installation root on Windows
  fi
  if [ -d "${miniconda_dir}" ]; then
    return 0  # already installed
  fi
  local miniconda_version="3-4.5.4" miniconda_platform="" exe_suffix=".sh"
  case "${OSTYPE}" in
    linux*) miniconda_platform=Linux;;
    darwin*) miniconda_platform=MacOSX;;
    msys*) miniconda_platform=Windows; exe_suffix=".exe";;
  esac
  local miniconda_url="https://repo.continuum.io/miniconda/Miniconda${miniconda_version}-${miniconda_platform}-${HOSTTYPE}${exe_suffix}"
  local miniconda_target="${HOME}/${miniconda_url##*/}"
  curl -s -L -o "${miniconda_target}" "${miniconda_url}"
  chmod +x "${miniconda_target}"
  case "${OSTYPE}" in
    msys*)
      MSYS2_ARG_CONV_EXCL="*" "${miniconda_target}" /S /D="$(cygpath -w -- "${miniconda_dir}")"
      ;;
    *)
      "${miniconda_target}" -b -p "${miniconda_dir}" | grep --line-buffered -v "^\(installing: \|installation finished\.\)"
      ;;
  esac
  reload_env
  if [ -n "${PYTHON-}" ]; then
    conda install -q -y python="${PYTHON}"
  fi
  python -m pip install --upgrade --quiet pip
}

install_nvm() {
  local NVM_HOME="${HOME}/.nvm"
  if [ "${OSTYPE}" = msys ]; then
    local version="1.1.7"
    if [ ! -f "${NVM_HOME}/nvm.sh" ]; then
      mkdir -p -- "${NVM_HOME}"
      export NVM_SYMLINK="${PROGRAMFILES}\nodejs"
      (
        cd "${NVM_HOME}"
        local target="./nvm-${version}.zip"
        curl -s -L -o "${target}" "https://github.com/coreybutler/nvm-windows/releases/download/${version}/nvm-noinstall.zip"
        unzip -q -- "${target}"
        rm -f -- "${target}"
        printf "%s\r\n" "root: $(cygpath -w -- "${NVM_HOME}")" "path: ${NVM_SYMLINK}" > settings.txt
      )
      printf "%s\n" "export NVM_HOME=\"$(cygpath -w -- "${NVM_HOME}")\"" 'nvm() { "${NVM_HOME}/nvm.exe" "$@"; }' > "${NVM_HOME}/nvm.sh"
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

  if "${python}" -m pip --version || "${python}" -m ensurepip; then  # If pip is present, configure it
    "${python}" -m pip install --upgrade --quiet pip

    # If we're in a CI environment, do some configuration
    if [ "${TRAVIS-}" = true ] || [ -n "${GITHUB_WORKFLOW-}" ]; then
      "${python}" -W ignore -m pip config -q --user set global.disable-pip-version-check True
      "${python}" -W ignore -m pip config -q --user set global.no-color True
      "${python}" -W ignore -m pip config -q --user set global.progress_bar off
      "${python}" -W ignore -m pip config -q --user set global.quiet True
    fi
  fi
}

install_node() {
  if [ "${OSTYPE}" = msys ]; then
    { echo "WARNING: Skipping running Node.js due to module incompatibilities with Windows"; } 2> /dev/null
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

run_npm() {
  npm ci
}

install_npm_project() {
  if [ "${OSTYPE}" = msys ]; then
    # Not Windows-compatible: https://github.com/npm/cli/issues/558#issuecomment-584673763
    { echo "WARNING: Skipping running NPM due to module incompatibilities with Windows"; } 2> /dev/null
  else
    run_npm
  fi
}

install_dependencies() {

  install_base
  if [ -n "${GITHUB_WORKFLOW-}" ]; then  # Keep Travis's built-in compilers and only use this for GitHub Actions (for now)
    "${ROOT_DIR}"/install-toolchains.sh
  fi
  install_nvm
  install_pip

  if [ -n "${PYTHON-}" ]; then
    install_miniconda

    # PyTorch is installed first since we are using a "-f" directive to find the wheels.
    # We want to install the CPU version only.
    case "${OSTYPE}" in
      linux*) pip install torch==1.5.0+cpu torchvision==0.6.0+cpu -f https://download.pytorch.org/whl/torch_stable.html;;
      darwin*) pip install torch torchvision;;
      msys*) pip install torch==1.5.0+cpu torchvision==0.6.0+cpu -f https://download.pytorch.org/whl/torch_stable.html;;
    esac

    pip_packages=(scipy tensorflow=="${TF_VERSION:-2.0.0b1}" cython==0.29.0 gym opencv-python-headless pyyaml \
      pandas==0.24.2 requests feather-format lxml openpyxl xlrd py-spy pytest pytest-timeout networkx tabulate aiohttp \
      uvicorn dataclasses pygments werkzeug kubernetes flask grpcio pytest-sugar pytest-rerunfailures pytest-asyncio \
      scikit-learn numba Pillow)
    if [ "${OSTYPE}" != msys ]; then
      # These packages aren't Windows-compatible
      pip_packages+=(blist)  # https://github.com/DanielStutzbach/blist/issues/81#issue-391460716
    fi
    CC=gcc pip install "${pip_packages[@]}"
  fi

  if [ "${LINT-}" = 1 ]; then
    install_miniconda
    pip install flake8==3.7.7 flake8-comprehensions flake8-quotes==2.0.0 yapf==0.23.0  # Python linters
    # readthedocs has an antiquated build env.
    # This is a best effort to reproduce it locally to avoid doc build failures and hidden errors.
    pip install -r "${WORKSPACE_DIR}"/doc/requirements-rtd.txt
    pip install -r "${WORKSPACE_DIR}"/doc/requirements-doc.txt
  fi

  # Install modules needed in all jobs.
  pip install dm-tree

  # Additional RLlib dependencies.
  if [ "${RLLIB_TESTING-}" = 1 ]; then
    pip install tensorflow-probability=="${TFP_VERSION-0.8}" gast==0.2.2 torch=="${TORCH_VERSION-1.4}" torchvision \
      atari_py gym[atari] lz4 smart_open
  fi

  # Additional streaming dependencies.
  if [ "${RAY_CI_STREAMING_PYTHON_AFFECTED}" = 1 ]; then
    pip install msgpack>=0.6.2
  fi

  if [ -n "${PYTHON-}" ] || [ -n "${LINT-}" ] || [ "${MAC_WHEELS-}" = 1 ]; then
    install_node
  fi

  CC=gcc pip install psutil setproctitle --target="${WORKSPACE_DIR}/python/ray/thirdparty_files"
}

install_dependencies "$@"

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)

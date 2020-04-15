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
        sudo usermod -a -G docker travis
      fi
      if [ -n "${PYTHON-}" ]; then
        "${ROOT_DIR}/install-strace.sh" || true
      fi
      ;;
  esac
}

install_miniconda() {
  local miniconda_version="3-4.5.4" miniconda_platform="" exe_suffix=".sh"
  case "${OSTYPE}" in
    linux*) miniconda_platform=Linux;;
    darwin*) miniconda_platform=MacOSX;;
    msys*) miniconda_platform=Windows; exe_suffix=".exe";;
  esac
  local miniconda_url="https://repo.continuum.io/miniconda/Miniconda${miniconda_version}-${miniconda_platform}-${HOSTTYPE}${exe_suffix}"
  local miniconda_target="./${miniconda_url##*/}"
  local miniconda_dir="$HOME/miniconda"
  curl -s -L -o "${miniconda_target}" "${miniconda_url}"
  chmod +x "${miniconda_target}"
  case "${OSTYPE}" in
    msys*)
      miniconda_dir="${miniconda_dir}/bin"  # HACK: Compensate for python.exe being in the installation root on Windows
      MSYS2_ARG_CONV_EXCL="*" "${miniconda_target}" /S /D="$(cygpath -w -- "${miniconda_dir}")"
      ;;
    *)
      "${miniconda_target}" -b -p "${miniconda_dir}" | grep --line-buffered -v "^\(installing: \|installation finished\.\)"
      ;;
  esac
  { local set_x="${-//[^x]/}"; } 2> /dev/null  # save set -x to suppress noise
  set +x
  local source_line='PYTHON3_BIN_PATH=python; PATH="$HOME/miniconda/bin:$PATH"; export PYTHON3_BIN_PATH PATH;'
  test -f ~/.bashrc && grep -x -q -F "${source_line}" -- ~/.bashrc || echo "${source_line}" >> ~/.bashrc
  test -z "${set_x}" || set -x  # restore set -x
  eval "${source_line}"
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

install_dependencies() {

  install_base
  if [ -n "${GITHUB_WORKFLOW-}" ]; then  # Keep Travis's built-in compilers and only use this for GitHub Actions (for now)
    "${ROOT_DIR}"/install-toolchains.sh
  fi
  install_nvm
  install_pip

  if [ -n "${PYTHON-}" ]; then
    install_miniconda
    pip_packages=(scipy tensorflow=="${TF_VERSION:-2.0.0b1}" cython==0.29.0 gym opencv-python-headless pyyaml \
      pandas==0.24.2 requests feather-format lxml openpyxl xlrd py-spy pytest pytest-timeout networkx tabulate aiohttp \
      uvicorn dataclasses pygments werkzeug kubernetes flask grpcio pytest-sugar pytest-rerunfailures pytest-asyncio \
      scikit-learn numba)
    if [ "${OSTYPE}" != msys ]; then
      # These packages aren't Windows-compatible
      pip_packages+=(blist)  # https://github.com/DanielStutzbach/blist/issues/81#issue-391460716
    fi
    CC=gcc pip install "${pip_packages[@]}"
  elif [ "${LINT-}" = 1 ]; then
    install_miniconda
    pip install flake8==3.7.7 flake8-comprehensions flake8-quotes==2.0.0  # Python linters
    pushd "${WORKSPACE_DIR}/python/ray/dashboard/client"  # TypeScript & HTML linters
      install_node
      run_npm
    popd
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

  if [ -n "${PYTHON-}" ] || [ "${MAC_WHEELS-}" = 1 ]; then
    install_node
  fi

  CC=gcc pip install psutil setproctitle --target="${WORKSPACE_DIR}/python/ray/thirdparty_files"
}

install_dependencies "$@"

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)

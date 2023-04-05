#!/usr/bin/env bash

# Push caller's shell options (quietly)
{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null

set -euxo pipefail

SCRIPT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
WORKSPACE_DIR="${SCRIPT_DIR}/../.."

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
  if command -v bazel; then
    if [[ -n "${BUILDKITE-}" ]] && [ "${OSTYPE}" != msys ]; then
      # Only reinstall Bazel if we need to upgrade to a different version.
      python="$(command -v python3 || command -v python || echo python)"
      current_version="$(bazel --version | grep -o "[0-9]\+.[0-9]\+.[0-9]\+")"
      new_version="$("${python}" -s -c "import runpy, sys; runpy.run_path(sys.argv.pop(), run_name='__api__')" bazel_version "${SCRIPT_DIR}/../../python/setup.py")"
      if [[ "$current_version" == "$new_version" ]]; then
        echo "Bazel of the same version already exists, skipping the install"
        export BAZEL_CONFIG_ONLY=1
      fi
    fi
  fi

  "${SCRIPT_DIR}"/install-bazel.sh
  if [ -f /etc/profile.d/bazel.sh ]; then
    . /etc/profile.d/bazel.sh
  fi
}

install_base() {
  if [ -n "${BUILDKITE-}" ]; then
    echo "Skipping install_base in Buildkite"
    return
  fi

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
        "${SCRIPT_DIR}/install-strace.sh" || true
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

  if [ ! -x "${conda}" ] || [ "${MINIMAL_INSTALL-}" = 1 ]; then  # If no conda is found, install it
    local miniconda_dir  # Keep directories user-independent, to help with Bazel caching
    local miniconda_version="Miniconda3-py37_4.9.2"
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
          miniconda_version="Miniconda3-py38_23.1.0-1"
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
  elif [ -n "${BUILDKITE-}" ]; then
    echo "Skipping nvm on Buildkite because we will use apt-get."
  else
    test -f "${NVM_HOME}/nvm.sh"  # double-check NVM is already available on other platforms
  fi
}

install_upgrade_pip() {
  local python=python
  if command -v python3 > /dev/null; then
    python=python3
  fi

  if "${python}" -m pip --version || "${python}" -m ensurepip; then  # Configure pip if present
    "${python}" -m pip install --upgrade pip

    # If we're in a CI environment, do some configuration
    if [ "${CI-}" = true ]; then
      "${python}" -W ignore -m pip config -q --user set global.disable-pip-version-check True
      "${python}" -W ignore -m pip config -q --user set global.progress_bar off
    fi

    "${python}" -m ensurepip
  fi
}

install_node() {
  if [ "${OSTYPE}" = msys ] ; then
    { echo "WARNING: Skipping running Node.js due to incompatibilities with Windows"; } 2> /dev/null
    return
  fi

  if [ -n "${BUILDKITE-}" ] ; then
    if [[ "${OSTYPE}" = darwin* ]]; then
      if [ "$(uname -m)" = "arm64" ]; then
        curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
      else
        curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
      fi
    else
      # https://github.com/nodesource/distributions/blob/master/README.md#installation-instructions
      curl -sL https://deb.nodesource.com/setup_14.x | sudo -E bash -
      sudo apt-get install -y nodejs
      return
    fi
  fi

  # Install the latest version of Node.js in order to build the dashboard.
  (
    set +x # suppress set -x since it'll get very noisy here.
    . "${HOME}/.nvm/nvm.sh"
    NODE_VERSION="14"
    nvm install $NODE_VERSION
    nvm use --silent $NODE_VERSION
    npm config set loglevel warn  # make NPM quieter
  )
}

install_toolchains() {
  if [ -z "${BUILDKITE-}" ]; then
    "${SCRIPT_DIR}"/install-toolchains.sh
  fi
  if [[ "${OSTYPE}" = linux* ]]; then
    pushd "${WORKSPACE_DIR}"
      "${SCRIPT_DIR}"/install-llvm-binaries.sh
    popd
  fi
}

download_mnist() {
  if [ -d "${HOME}/data/MNIST" ]; then
    return
  fi
  mkdir -p "${HOME}/data"
  curl -o "${HOME}/data/mnist.zip" https://ray-ci-mnist.s3-us-west-2.amazonaws.com/mnist.zip
  unzip "${HOME}/data/mnist.zip" -d "${HOME}/data"
}

retry_pip_install() {
  local pip_command=$1
  local status="0"
  local errmsg=""

  # Try n times; we often encounter OpenSSL.SSL.WantReadError (or others)
  # that break the entire CI job: Simply retry installation in this case
  # after n seconds.
  for _ in {1..3}; do
    errmsg=$(eval "${pip_command}" 2>&1) && break
    status=$errmsg && echo "'pip install ...' failed, will retry after n seconds!" && sleep 30
  done
  if [ "$status" != "0" ]; then
    echo "${status}" && return 1
  fi
}

compile_ray_requirements() {
  # Compile unpinned python/requirements.txt into python/requirements_pinned.txt
  # shellcheck disable=SC2262
  alias pip="python -m pip"
  pip install pip-tools

  if [ -f "${WORKSPACE_DIR}/python/requirements_pinned.txt" ]; then
    echo requirements_pinned already exists
  else
    pip-compile --resolver=backtracking -q \
       --pip-args --no-deps --strip-extras --no-annotate --no-header -q -o \
      "${WORKSPACE_DIR}/python/requirements_pinned.txt" \
      "${WORKSPACE_DIR}/python/requirements.txt" \
      "${WORKSPACE_DIR}/python/requirements_test.txt" \
      "${WORKSPACE_DIR}/python/requirements_linters.txt"
  fi

  if [ -n "${BUILDKITE-}" ] && [ -d "/artifact-mount" ]; then
    rm -rf /artifact-mount/requirements*.txt
    cp -f "${WORKSPACE_DIR}/python/requirements_pinned.txt" /artifact-mount/
  else
    if [ -n "${BUILDKITE-}" ]; then
      mkdir -p /tmp/artifacts
      rm -rf /tmp/artifacts/requirements*.txt
      cp -f "${WORKSPACE_DIR}/python/requirements_pinned.txt" /tmp/artifacts/
    fi
    cat "${WORKSPACE_DIR}/python/requirements_pinned.txt"
  fi
}

install_pip_packages() {
  # Install modules needed in all jobs.
  # shellcheck disable=SC2262
  alias pip="python -m pip"

  # Pin requirements
  compile_ray_requirements

  # Array to hold all requirements files to install later
  requirements_files=()
  requirements_packages=()

  requirements_files+=("${WORKSPACE_DIR}/python/requirements_test.txt")

  if [ "${LINT-}" = 1 ]; then
    install_linters

    requirements_files+=("${WORKSPACE_DIR}/doc/requirements-doc.txt")
  fi

  # Additional default doc testing dependencies.
  if [ "${DOC_TESTING-}" = 1 ]; then
    # For Ray Core and Ray Serve DAG visualization docs test + dataset examples
    sudo apt-get install -y graphviz tesseract-ocr

    # For DAG visualization
    requirements_packages+=("pydot")
    requirements_packages+=("pytesseract")
    requirements_packages+=("spacy>=3")
    requirements_packages+=("spacy_langdetect")
  fi

  # Additional RLlib test dependencies.
  if [ "${RLLIB_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/requirements_rllib.txt")
    #TODO(amogkam): Add this back to requirements_rllib.txt once mlagents no longer pins torch<1.9.0 version.
    pip install --no-dependencies mlagents==0.28.0
  fi

  local install_ml_no_deps=0

  # Additional Train test dependencies.
  if [ "${TRAIN_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/requirements_train.txt")
    install_ml_no_deps=1
  fi

  # Additional Tune/Doc test dependencies.
  if [ "${TUNE_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/requirements_tune.txt")
  fi

  # For Tune, install upstream dependencies.
  if [ "${TUNE_TESTING-}" = 1 ] ||  [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/requirements_upstream.txt")
  fi

  # Additional dependency for Ludwig.
  # This cannot be included in requirements_upstream.txt as it has conflicting
  # dependencies with Modin.
  if [ "${INSTALL_LUDWIG-}" = 1 ]; then
    # TODO: eventually pin this to master.
    requirements_packages+=("ludwig[test]>=0.4")
    requirements_packages+=("jsonschema>=4")
  fi

  # Additional dependency for time series libraries.
  # This cannot be included in requirements_tune.txt as it has conflicting
  # dependencies.
  if [ "${INSTALL_TIMESERIES_LIBS-}" = 1 ]; then
    requirements_packages+=("statsforecast==1.1.0")
    requirements_packages+=("prophet==1.1.1")
  fi

  # Data processing test dependencies.
  if [ "${DATA_PROCESSING_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/data_processing/requirements.txt")
  fi
  if [ "${DATA_PROCESSING_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/data_processing/requirements_dataset.txt")
    if [ -n "${ARROW_VERSION-}" ]; then
      if [ "${ARROW_VERSION-}" = nightly ]; then
        pip install --extra-index-url https://pypi.fury.io/arrow-nightlies/ --prefer-binary --pre pyarrow
      else
        requirements_packages+=("pyarrow==${ARROW_VERSION}")
      fi
    fi
    if [ -n "${ARROW_MONGO_VERSION-}" ]; then
      requirements_packages+=("pymongoarrow==${ARROW_MONGO_VERSION}")
    fi
  fi

  if [ "${install_ml_no_deps}" = 1 ]; then
    pip install -r "${WORKSPACE_DIR}/python/requirements/ml/requirements_no_deps.txt"
  fi

  retry_pip_install "CC=gcc pip install -Ur ${WORKSPACE_DIR}/python/requirements_pinned.txt"

  # Remove this entire section once Serve dependencies are fixed.
  if [ "${DOC_TESTING-}" != 1 ] && [ "${TRAIN_TESTING-}" != 1 ] && [ "${TUNE_TESTING-}" != 1 ] && [ "${RLLIB_TESTING-}" != 1 ]; then
    # If CI has deemed that a different version of Torch
    # should be installed, then upgrade/downgrade to that specific version.
    if [ -n "${TORCH_VERSION-}" ]; then
      case "${TORCH_VERSION-1.9.0}" in
        1.9.0) TORCHVISION_VERSION=0.10.0;;
        1.8.1) TORCHVISION_VERSION=0.9.1;;
        1.6) TORCHVISION_VERSION=0.7.0;;
        1.5) TORCHVISION_VERSION=0.6.0;;
        *) TORCHVISION_VERSION=0.5.0;;
      esac
      requirements_packages+=("torch==${TORCH_VERSION-1.9.0}")
      requirements_packages+=("torchvision==${TORCHVISION_VERSION}")
      # Install right away - some torch dependencies (e.g. torch-spline-conv)
      # need an existing torch for dependency resolution
      pip install -U "torch==${TORCH_VERSION-1.9.0}" "torchvision==${TORCHVISION_VERSION}"
    fi
  elif [ "${DL-}" = "1" ] || [ "${RLLIB_TESTING-}" = 1 ] || [ "${TRAIN_TESTING-}" = 1 ] || [ "${TUNE_TESTING-}" = 1 ]; then
      # E.g. torch-spline-conv needs to import torch in their setup.py
      # so it has to be installed first
      # (see https://github.com/ray-project/ray/pull/33928)
      # Keep in sync with requirements_dl.txt
      pip install torch==1.13.0 torchvision==0.14.0
      requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/requirements_dl.txt")
  fi

  # Inject our own mirror for the CIFAR10 dataset
  if [ "${TRAIN_TESTING-}" = 1 ] || [ "${TUNE_TESTING-}" = 1 ] ||  [ "${DOC_TESTING-}" = 1 ]; then
    SITE_PACKAGES=$(python -c 'from distutils.sysconfig import get_python_lib; print(get_python_lib())')

    TF_CIFAR="${SITE_PACKAGES}/tensorflow/python/keras/datasets/cifar10.py"
    TORCH_CIFAR="${SITE_PACKAGES}/torchvision/datasets/cifar.py"

    [ -f "$TF_CIFAR" ] && sed -i 's https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz https://air-example-data.s3.us-west-2.amazonaws.com/cifar-10-python.tar.gz g' \
      "$TF_CIFAR"
    [ -f "$TORCH_CIFAR" ] &&sed -i 's https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz https://air-example-data.s3.us-west-2.amazonaws.com/cifar-10-python.tar.gz g' \
      "$TORCH_CIFAR"
  fi

  # Generate the pip command with collected requirements files
  pip_cmd="pip install -U -c ${WORKSPACE_DIR}/python/requirements_pinned.txt"
  for file in "${requirements_files[@]}"; do
    eval "${pip_cmd}" -r "${file}"
    # pip_cmd+=" -r ${file}"
  done

  pip_add=""
  for package in "${requirements_packages[@]}"; do
    pip_add+=" ${package}"
  done

  if [ -n "${pip_add}" ]; then
    pip_cmd="pip install $pip_add"
    # Run the pip command to install all collected requirements files
    echo Running pip install: "$pip_cmd"
    eval "${pip_cmd}"
  fi

  # Additional Tune dependency for Horovod.
  # This must be run last (i.e., torch cannot be re-installed after this)
  if [ "${INSTALL_HOROVOD-}" = 1 ]; then
    "${SCRIPT_DIR}"/install-horovod.sh
  fi

  if [ "${TUNE_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    download_mnist
  fi

  if [ "${DOC_TESTING-}" = 1 ]; then
    python -m spacy download en_core_web_sm
  fi
}

install_thirdparty_packages() {
  # shellcheck disable=SC2262
  alias pip="python -m pip"
  CC=gcc pip install psutil setproctitle==1.2.2 colorama --target="${WORKSPACE_DIR}/python/ray/thirdparty_files"
}

install_dependencies() {
  install_bazel

  # Only install on buildkite if requested
  if [ -z "${BUILDKITE-}" ] || [ "${BUILD-}" = "1" ]; then
    install_base
    install_toolchains
  fi

  if [ -n "${PYTHON-}" ] || [ "${LINT-}" = 1 ] || [ "${MINIMAL_INSTALL-}" = "1" ]; then
    install_miniconda
  fi

  install_upgrade_pip

  # Only install on buildkite if requested
  if [ -z "${BUILDKITE-}" ] || [ "${BUILD-}" = "1" ]; then
    install_nvm
    if [ -n "${PYTHON-}" ] || [ -n "${LINT-}" ] || [ "${MAC_WHEELS-}" = 1 ]; then
      install_node
    fi
  fi

  # install hdfs if needed.
  if [ "${INSTALL_HDFS-}" = 1 ]; then
    "${SCRIPT_DIR}"/install-hdfs.sh
  fi

  if [ "${MINIMAL_INSTALL-}" != "1" ]; then
    install_pip_packages
  fi

  install_thirdparty_packages
}

install_dependencies "$@"

# Pop caller's shell options (quietly)
{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null

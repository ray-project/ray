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
      new_version="$(grep 'USE_BAZEL_VERSION=' "${WORKSPACE_DIR}/.bazeliskrc" | grep -o "[0-9]\+.[0-9]\+.[0-9]\+")"
      if [[ "$current_version" == "$new_version" ]]; then
        echo "Bazel of the same version already exists, skipping the install"
        export BAZEL_CONFIG_ONLY=1
      fi
    fi
  fi

  "${SCRIPT_DIR}"/install-bazel.sh
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
    local miniconda_version="Miniconda3-py38_23.1.0-1"
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
  pip install -r "${WORKSPACE_DIR}"/python/requirements/lint-requirements.txt

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

install_pip_packages() {
  # Install modules needed in all jobs.
  # shellcheck disable=SC2262
  alias pip="python -m pip"

  # Array to hold all requirements files to install later
  requirements_files=()
  # Single packages to install in sync with files
  requirements_packages=()
  # Packages to install _after_ previous files have been installed
  # (e.g. to install a custom pyarrow or torch version). This
  # would otherwise conflict with pinned dependencies in our requirements
  # files.
  delayed_packages=()

  requirements_files+=("${WORKSPACE_DIR}/python/requirements/test-requirements.txt")

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
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/rllib-requirements.txt")
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/rllib-test-requirements.txt")
    #TODO(amogkam): Add this back to rllib-requirements.txt once mlagents no longer pins torch<1.9.0 version.
    pip install --no-dependencies mlagents==0.28.0

    # Install MuJoCo.
    sudo apt install libosmesa6-dev libgl1-mesa-glx libglfw3 patchelf -y
    wget https://mujoco.org/download/mujoco210-linux-x86_64.tar.gz
    mkdir -p /root/.mujoco
    mv mujoco210-linux-x86_64.tar.gz /root/.mujoco/.
    (cd /root/.mujoco && tar -xf /root/.mujoco/mujoco210-linux-x86_64.tar.gz)
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/root/.mujoco/mujoco210/bin
  fi

  # Additional Train test dependencies.
  if [ "${TRAIN_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/train-requirements.txt")
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/train-test-requirements.txt")
  fi

  # Additional Tune/Doc test dependencies.
  if [ "${TUNE_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/tune-requirements.txt")
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/tune-test-requirements.txt")
  fi

  # Additional dependency for Ludwig.
  # This cannot be included in requirements files as it has conflicting
  # dependencies with Modin.
  if [ "${INSTALL_LUDWIG-}" = 1 ]; then
    # TODO: eventually pin this to master.
    requirements_packages+=("ludwig[test]>=0.4")
    requirements_packages+=("jsonschema>=4")
  fi

  # Additional dependency for time series libraries.
  # This cannot be included in tune-requirements.txt as it has conflicting
  # dependencies.
  if [ "${INSTALL_TIMESERIES_LIBS-}" = 1 ]; then
    requirements_packages+=("statsforecast==1.5.0")
    requirements_packages+=("prophet==1.1.1")
    requirements_packages+=("holidays==0.24") # holidays 0.25 causes `import prophet` to fail.
  fi

  # Data processing test dependencies.
  if [ "${DATA_PROCESSING_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/data-requirements.txt")
  fi
  if [ "${DATA_PROCESSING_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/data-test-requirements.txt")
    if [ -n "${ARROW_VERSION-}" ]; then
      if [ "${ARROW_VERSION-}" = nightly ]; then
        delayed_packages+=("--extra-index-url")
        delayed_packages+=("https://pypi.fury.io/arrow-nightlies/")
        delayed_packages+=("--prefer-binary")
        delayed_packages+=("--pre")
        delayed_packages+=("pyarrow")
      else
        delayed_packages+=("pyarrow==${ARROW_VERSION}")
      fi
    fi
    if [ -n "${ARROW_MONGO_VERSION-}" ]; then
      delayed_packages+=("pymongoarrow==${ARROW_MONGO_VERSION}")
    fi
  fi

  retry_pip_install "CC=gcc pip install -Ur ${WORKSPACE_DIR}/python/requirements.txt"

  # Install deeplearning libraries (Torch + TensorFlow)
  if [ -n "${TORCH_VERSION-}" ] || [ "${DL-}" = "1" ] || [ "${RLLIB_TESTING-}" = 1 ] || [ "${TRAIN_TESTING-}" = 1 ] || [ "${TUNE_TESTING-}" = 1 ]; then
      # If we require a custom torch version, use that
      if [ -n "${TORCH_VERSION-}" ]; then
        # Install right away, as some dependencies (e.g. torch-spline-conv) need
        # torch to be installed for their own install.
        pip install -U "torch==${TORCH_VERSION-1.9.0}" "torchvision==${TORCHVISION_VERSION-0.10.0}"
        # We won't add dl-cpu-requirements.txt as it would otherwise overwrite our custom
        # torch. Thus we have also have to install tensorflow manually.
        TF_PACKAGE=$(grep -ohE "tensorflow==[^ ;]+" "${WORKSPACE_DIR}/python/requirements/ml/dl-cpu-requirements.txt" | head -n 1)
        TFPROB_PACKAGE=$(grep -ohE "tensorflow-probability==[^ ;]+" "${WORKSPACE_DIR}/python/requirements/ml/dl-cpu-requirements.txt" | head -n 1)

        # %%;* deletes everything after ; to get rid of e.g. python version specifiers
        pip install -U "${TF_PACKAGE%%;*}" "${TFPROB_PACKAGE%%;*}"
      else
        # Otherwise, use pinned default torch version.
        # Again, install right away, as some dependencies (e.g. torch-spline-conv) need
        # torch to be installed for their own install.
        TORCH_PACKAGE=$(grep -ohE "torch==[^ ;]+" "${WORKSPACE_DIR}/python/requirements/ml/dl-cpu-requirements.txt" | head -n 1)
        TORCHVISION_PACKAGE=$(grep -ohE "torchvision==[^ ;]+" "${WORKSPACE_DIR}/python/requirements/ml/dl-cpu-requirements.txt" | head -n 1)

        # %%;* deletes everything after ; to get rid of e.g. python version specifiers
        pip install "${TORCH_PACKAGE%%;*}" "${TORCHVISION_PACKAGE%%;*}"
        requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/dl-cpu-requirements.txt")
      fi
  fi

  # AIR core dependencies
  if [ "${RLLIB_TESTING-}" = 1 ] || [ "${TRAIN_TESTING-}" = 1 ] || [ "${TUNE_TESTING-}" = 1 ] || [ "${DOC_TESTING-}" = 1 ]; then
    requirements_files+=("${WORKSPACE_DIR}/python/requirements/ml/core-requirements.txt")
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
  pip_cmd="pip install -U -c ${WORKSPACE_DIR}/python/requirements.txt"

  if [[ -f "${WORKSPACE_DIR}/python/requirements_compiled.txt"  &&  "${PYTHON-}" != "3.7" && "${OSTYPE}" != msys ]]; then
    # On Python 3.7, we don't, as the dependencies are compiled for 3.8+
    # and we don't build ray-ml images. This means we don't have to keep
    # consistency between CI and docker images.
    # On Windows, some pinned dependencies are not built for win, so we
    # skip this until we have a good wy to resolve cross-platform dependencies.
    pip_cmd+=" -c ${WORKSPACE_DIR}/python/requirements_compiled.txt"
  fi

  for file in "${requirements_files[@]}"; do
     pip_cmd+=" -r ${file}"
  done

  # Expand single requirements
  if [ "${#requirements_packages[@]}" -gt 0 ]; then
    pip_cmd+=" ${requirements_packages[*]}"
  fi

  # Install
  eval "${pip_cmd}"

  # Install delayed packages
  if [ "${#delayed_packages[@]}" -gt 0 ]; then
    pip install -U -c "${WORKSPACE_DIR}/python/requirements.txt" "${delayed_packages[@]}"
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
    # Todo: This downgrades spacy and related dependencies because
    # `en_core_web_sm` is only compatible with spacy < 3.6.
    # We should move to a model that does not depend on a stale version.
    python -m spacy download en_core_web_sm
  fi
}

install_thirdparty_packages() {
  mkdir -p "${WORKSPACE_DIR}/python/ray/thirdparty_files"
  RAY_THIRDPARTY_FILES="$(realpath "${WORKSPACE_DIR}/python/ray/thirdparty_files")"
  CC=gcc python -m pip install psutil setproctitle==1.2.2 colorama --target="${RAY_THIRDPARTY_FILES}"
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

install_dependencies

# Pop caller's shell options (quietly)
{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null

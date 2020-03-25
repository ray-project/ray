#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

# Make sure all important package versions are static (via env variables
# or assign default values to them).
tf_version="$TF_VERSION"
if [[ $tf_version == "" ]]; then tf_version="2.0.0b1"; fi
echo "tf_version is $tf_version"
tfp_version="$TFP_VERSION"
if [[ tfp_version == "" ]]; then tfp_version="0.8"; fi
echo "tfp_version is $tfp_version"
torch_version="$TORCH_VERSION"
if [[ torch_version == "" ]]; then torch_version="1.4"; fi
echo "torch_version is $torch_version"

platform="unknown"
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  echo "Platform is linux."
  platform="linux"
elif [[ "$unamestr" == "Darwin" ]]; then
  echo "Platform is macosx."
  platform="macosx"
else
  echo "Unrecognized platform."
  exit 1
fi

# Upgrade pip and other packages to avoid incompatibility ERRORS.
pip install --upgrade pip # setuptools cloudpickle urllib3

# If we're in a CI environment, do some configuration
if [ "${TRAVIS-}" = true ] || [ -n "${GITHUB_WORKFLOW-}" ]; then
  pip config --user set global.disable-pip-version-check True
  pip config --user set global.no-color True
  pip config --user set global.progress_bar off
  pip config --user set global.quiet True
fi

if [[ "$PYTHON" == "3.6" ]] && [[ "$platform" == "linux" ]]; then
  sudo apt-get update
  sudo apt-get install -y build-essential curl unzip tmux gdb libunwind-dev
  # Install miniconda.
  wget -q https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  "${ROOT_DIR}/install-strace.sh" || true
  pip install scipy tensorflow==$tf_version \
    cython==0.29.0 gym \
    opencv-python-headless pyyaml pandas==0.24.2 requests \
    feather-format lxml openpyxl xlrd py-spy pytest pytest-timeout networkx tabulate aiohttp \
    uvicorn dataclasses pygments werkzeug kubernetes flask grpcio pytest-sugar pytest-rerunfailures pytest-asyncio \
    blist scikit-learn numba
elif [[ "$PYTHON" == "3.6" ]] && [[ "$platform" == "macosx" ]]; then
  # Install miniconda.
  wget -q https://repo.continuum.io/miniconda/Miniconda3-4.5.4-MacOSX-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install scipy tensorflow==$tf_version \
    cython==0.29.0 gym \
    opencv-python-headless pyyaml pandas==0.24.2 requests \
    feather-format lxml openpyxl xlrd py-spy pytest pytest-timeout networkx tabulate aiohttp \
    uvicorn dataclasses pygments werkzeug kubernetes flask grpcio pytest-sugar pytest-rerunfailures pytest-asyncio \
    blist scikit-learn numba
elif [[ "$LINT" == "1" ]]; then
  sudo apt-get update
  sudo apt-get install -y build-essential curl unzip
  # Install miniconda.
  wget -q https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  # Install Python linting tools.
  pip install flake8==3.7.7 flake8-comprehensions flake8-quotes==2.0.0
  # Install TypeScript and HTML linting tools.
  pushd "$ROOT_DIR/../../python/ray/dashboard/client"
    source "$HOME/.nvm/nvm.sh"
    nvm install node
    nvm use node
    npm ci
  popd
elif [[ "$LINUX_WHEELS" == "1" ]]; then
  sudo apt-get install docker
  sudo usermod -a -G docker travis
elif [[ "$MAC_WHEELS" == "1" ]]; then
  :
else
  echo "Unrecognized environment."
  exit 1
fi

# Install modules needed in all jobs.
pip install dm-tree

# Additional RLlib dependencies.
if [[ "$RLLIB_TESTING" == "1" ]]; then
  pip install tensorflow-probability==$tfp_version gast==0.2.2 \
    torch==$torch_version torchvision \
    atari_py gym[atari] lz4 smart_open
fi

# Additional streaming dependencies.
if [[ "$RAY_CI_STREAMING_PYTHON_AFFECTED" == "1" ]]; then
  pip install -q msgpack>=0.6.2
fi

if [[ "$PYTHON" == "3.6" ]] || [[ "$MAC_WHEELS" == "1" ]]; then
  # Install the latest version of Node.js in order to build the dashboard.
  source "$HOME/.nvm/nvm.sh"
  nvm install node
fi

pip install psutil setproctitle \
        --target="$ROOT_DIR/../../python/ray/thirdparty_files"

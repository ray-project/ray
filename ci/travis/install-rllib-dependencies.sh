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

if [[ "$platform" == "linux" ]]; then
  sudo apt-get update
  sudo apt-get install -y python-dev python-numpy build-essential curl unzip tmux gdb
  # Install miniconda.
  wget -q https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
elif [[ "$platform" == "macos" ]]; then
  # Install miniconda.
  wget -q https://repo.continuum.io/miniconda/Miniconda3-4.5.4-MacOSX-x86_64.sh -O miniconda.sh -nv
else
  echo "Unrecognized environment."
  exit 1
fi

bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"

# Upgrade pip and other packages to avoid incompatibility ERRORS.
pip install --upgrade pip setuptools cloudpickle urllib3

# pip-install all required packages.
pip install -q scipy cython==0.29.0 \
  tensorflow==$tf_version tensorflow-probability==$tfp_version \
  torch==$torch_version torchvision \
  gast==0.2.2 gym gym[atari] atari_py opencv-python-headless pyyaml \
  pandas==0.24.2 requests feather-format lxml lz4 \
  openpyxl xlrd py-spy pytest-timeout networkx tabulate \
  aiohttp uvicorn dataclasses pygments werkzeug kubernetes flask grpcio \
  pytest-sugar pytest-rerunfailures pytest-asyncio blist \
  scikit-learn

pip install -q psutil setproctitle \
        --target="$ROOT_DIR/../../python/ray/thirdparty_files"

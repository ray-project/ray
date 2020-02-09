#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "PYTHON is $PYTHON"

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
pip install -q scipy tensorflow==2.0.0b1 tensorflow-probability==0.8 gast==0.2.2 \
  cython==0.29.0 \
  gym gym[atari] atari_py \
  opencv-python-headless pyyaml pandas==0.24.2 requests feather-format lxml \
  openpyxl xlrd py-spy setproctitle pytest-timeout networkx tabulate psutil \
  aiohttp uvicorn dataclasses pygments werkzeug kubernetes flask grpcio \
  pytest-sugar pytest-rerunfailures pytest-asyncio blist torch torchvision \
  scikit-learn

pip install -q psutil setproctitle \
        --target="$ROOT_DIR/../../python/ray/thirdparty_files"

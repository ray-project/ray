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

if [[ "$PYTHON" == "2.7" ]] && [[ "$platform" == "linux" ]]; then
  sudo apt-get update
  sudo apt-get install -y build-essential curl python-dev python-numpy python-pip unzip tmux gdb
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda2-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install -q scipy tensorflow cython==0.29.0 gym opencv-python-headless pyyaml pandas==0.23.4 requests \
    feather-format lxml openpyxl xlrd py-spy setproctitle faulthandler pytest-timeout mock flaky networkx tabulate psutil
elif [[ "$PYTHON" == "3.5" ]] && [[ "$platform" == "linux" ]]; then
  sudo apt-get update
  sudo apt-get install -y python-dev python-numpy build-essential curl unzip tmux gdb
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install -q scipy tensorflow cython==0.29.0 gym opencv-python-headless pyyaml pandas==0.23.4 requests \
    feather-format lxml openpyxl xlrd py-spy setproctitle pytest-timeout flaky networkx tabulate psutil aiohttp
elif [[ "$PYTHON" == "2.7" ]] && [[ "$platform" == "macosx" ]]; then
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda2-4.5.4-MacOSX-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install -q cython==0.29.0 tensorflow gym opencv-python-headless pyyaml pandas==0.23.4 requests \
    feather-format lxml openpyxl xlrd py-spy setproctitle faulthandler pytest-timeout mock flaky networkx tabulate psutil
elif [[ "$PYTHON" == "3.5" ]] && [[ "$platform" == "macosx" ]]; then
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-4.5.4-MacOSX-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  pip install -q cython==0.29.0 tensorflow gym opencv-python-headless pyyaml pandas==0.23.4 requests \
    feather-format lxml openpyxl xlrd py-spy setproctitle pytest-timeout flaky networkx tabulate psutil aiohttp
elif [[ "$LINT" == "1" ]]; then
  sudo apt-get update
  sudo apt-get install -y build-essential curl unzip
  # Install miniconda.
  wget https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda.sh -nv
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
  # Install Python linting tools.
  pip install -q flake8 flake8-comprehensions
  pip install flake8-quotes==2.0.0
elif [[ "$LINUX_WHEELS" == "1" ]]; then
  sudo apt-get install docker
  sudo usermod -a -G docker travis
elif [[ "$MAC_WHEELS" == "1" ]]; then
  :
else
  echo "Unrecognized environment."
  exit 1
fi
